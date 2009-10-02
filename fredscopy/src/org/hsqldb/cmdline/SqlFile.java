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
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Method;
import org.hsqldb.lib.ValidatingResourceBundle;
import org.hsqldb.lib.AppendableException;
import org.hsqldb.lib.RCData;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.cmdline.sqltool.Token;
import org.hsqldb.cmdline.sqltool.TokenList;
import org.hsqldb.cmdline.sqltool.TokenSource;
import org.hsqldb.cmdline.sqltool.SqlFileScanner;
import org.hsqldb.types.Types;

/* $Id$ */

/**
 * Encapsulation of a sql text file like 'myscript.sql'.
 * The ultimate goal is to run the execute() method to feed the SQL
 * commands within the file to a jdbc connection.
 * <P/>
 * The file <CODE>src/org/hsqldb/sample/SqlFileEmbedder.java</CODE>
 * in the HSQLDB distribution provides an example for using SqlFile to
 * execute SQL files directly from your own Java classes.
 * <P/>
 * The complexities of passing userVars and macros maps are to facilitate
 * strong scoping (among blocks and nested scripts).
 * <P/>
 * Some implementation comments and variable names use keywords based
 * on the following definitions.  <UL>
 * <LI> COMMAND = Statement || SpecialCommand || BufferCommand
 * <LI>Statement = SQL statement like "SQL Statement;"
 * <LI>SpecialCommand =  Special Command like "\x arg..."
 * <LI>BufferCommand =  Editing/buffer command like ":s/this/that/"
 * </UL>
 * <P/>
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
 * <P/>
 * In general, the special commands mirror those of Postgresql's psql,
 * but SqlFile handles command editing much different from Postgresql
 * because of Java's lack of support for raw tty I/O.
 * The \p special command, in particular, is very different from psql's.
 * <P/>
 * Buffer commands are unique to SQLFile.  The ":" commands allow
 * you to edit the buffer and to execute the buffer.
 * <P/>
 * \d commands are very poorly supported for Mysql because
 * (a) Mysql lacks most of the most basic JDBC support elements, and
 * the most basic role and schema features, and
 * (b) to access the Mysql data dictionay, one must change the database
 * instance (to do that would require work to restore the original state
 * and could have disastrous effects upon transactions).
 * <P/>
 * To make changes to this class less destructive to external callers,
 * the input parameters should be moved to setters (probably JavaBean
 * setters would be best) instead of constructor args and System
 * Properties.
 * <P/>
 * The process*() methods, other than processBuffHist() ALWAYS execute
 * on "buffer", and expect it to contain the method specific prefix
 * (if any).
 *
 * @see <a href="../../../../util-guide/sqltool-chapt.html" target="guide">
 *     The SqlTool chapter of the
 *     HyperSQL Utilities Guide</a>
 * @see org.hsqldb.sample.SqlFileEmbedder
 * @version $Revision$, $Date$
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */

public class SqlFile {
    static private FrameworkLogger logger =
            FrameworkLogger.getLog(SqlFile.class);
    private static final int DEFAULT_HISTORY_SIZE = 40;
    private boolean permitEmptySqlStatements = false;
    private File             file;
    private boolean          interactive;
    private String           primaryPrompt    = "sql> ";
    private String           rawPrompt        = null;
    private String           contPrompt       = "  +> ";
    private Connection       curConn          = null;
    private boolean          htmlMode         = false;
    private Map              userVars; // Always a non-null map set in cons.
    private Map              macros; // Always a non-null map set in cons.
    private List             history          = null;
    private String           nullRepToken     = null;
    private String           dsvTargetFile    = null;
    private String           dsvTargetTable   = null;
    private String           dsvConstCols     = null;
    private String           dsvRejectFile    = null;
    private String           dsvRejectReport  = null;
    private int              dsvRecordsPerCommit = 0;
    public static String     LS = System.getProperty("line.separator");
    private int              maxHistoryLength = 1;
    private SqltoolRB        rb               = null;
    private boolean          reportTimes      = false;

    /**
     * N.b. javax.util.regex Optional capture groups (...)? are completely
     * unpredictable wrt whether you get a null capture group vs. no capture.
     * Must always check count!
     */
    private static Pattern   specialPattern =
            Pattern.compile("(\\S+)(?:\\s+(.*\\S))?\\s*");
    private static Pattern   plPattern  = Pattern.compile("(.*\\S)?\\s*");
    private static Pattern   foreachPattern =
            Pattern.compile("foreach\\s+(\\S+)\\s*\\(([^)]+)\\)\\s*");
    private static Pattern   ifwhilePattern =
            Pattern.compile("\\S+\\s*\\(([^)]*)\\)\\s*");
    private static Pattern   varsetPattern =
            Pattern.compile("(\\S+)\\s*([=_~])\\s*(?:(.*\\S)\\s*)?");
    private static Pattern   substitutionPattern =
            Pattern.compile("(\\S)(.+?)\\1(.*?)\\1(.+)?\\s*");
            // Note that this pattern does not include the leading ":s".
    private static Pattern   slashHistoryPattern =
            Pattern.compile("\\s*/([^/]+)/\\s*(\\S.*)?");
    private static Pattern   historyPattern =
            Pattern.compile("\\s*(-?\\d+)?\\s*(\\S.*)?");
            // Note that this pattern does not include the leading ":".
    private static Pattern wincmdPattern = null;
    private static Pattern useMacroPattern =
            Pattern.compile("(\\w+)(\\s.*[^;])?(;?)");
    private static Pattern editMacroPattern =
            Pattern.compile("(\\w+)\\s*:(.*)");
    private static Pattern spMacroPattern =
            Pattern.compile("(\\w+)\\s+([*\\\\])(.*\\S)");
    private static Pattern sqlMacroPattern =
            Pattern.compile("(\\w+)\\s+(.*\\S)");
    private static Pattern integerPattern = Pattern.compile("\\d+");
    private static Pattern nameValPairPattern =
            Pattern.compile("\\s*(\\w+)\\s*=(.*)");
            // Specifically permits 0-length values, but not names.
    private static Pattern nameDotPattern = Pattern.compile("(\\w+)\\.");
    private static Pattern commitOccursPattern =
            Pattern.compile("(?is)(?:set\\s+autocommit.*)|(commit\\s*)");
    private static Pattern logPattern =
        Pattern.compile("(?i)(FINER|WARNING|SEVERE|INFO|FINEST)\\s+(.*\\S)");

    static private Map nestingPLCommands = new HashMap();
    static {
        nestingPLCommands.put("if", ifwhilePattern);
        nestingPLCommands.put("while", ifwhilePattern);
        nestingPLCommands.put("foreach", foreachPattern);
    }

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
        dsvSkipCols = (String) userVars.get("*DSV_SKIP_COLS");
        dsvTrimAll = Boolean.valueOf((String) userVars.get("*DSV_TRIM_ALL")).
                booleanValue();
        dsvColDelim =
            SqlFile.convertEscapes((String) userVars.get("*DSV_COL_DELIM"));
        if (dsvColDelim == null) {
            dsvColDelim =
                SqlFile.convertEscapes((String) userVars.get("*CSV_COL_DELIM"));
        }
        if (dsvColDelim == null) {
            dsvColDelim = DEFAULT_COL_DELIM;
        }
        dsvColSplitter = (String) userVars.get("*DSV_COL_SPLITTER");
        if (dsvColSplitter == null) {
            dsvColSplitter = DEFAULT_COL_SPLITTER;
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
        dsvRowSplitter = (String) userVars.get("*DSV_ROW_SPLITTER");
        if (dsvRowSplitter == null) {
            dsvRowSplitter = DEFAULT_ROW_SPLITTER;
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
        if (userVars.get("*DSV_RECORDS_PER_COMMIT") != null) try {
            dsvRecordsPerCommit = Integer.parseInt(
                    (String) userVars.get("*DSV_RECORDS_PER_COMMIT"));
        } catch (NumberFormatException nfe) {
            logger.error(rb.getString(SqltoolRB.REJECT_RPC,
                    (String) userVars.get("*DSV_RECORDS_PER_COMMIT")));
            userVars.remove("*DSV_REJECT_REPORT");
            dsvRecordsPerCommit = 0;
        }

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
        BooleanBucket() {
            // Purposefully empty
        }
        private boolean bPriv = false;

        public void set(boolean bIn) {
            bPriv = bIn;
        }

        public boolean get() {
            return bPriv;
        }
    }

    BooleanBucket possiblyUncommitteds = new BooleanBucket();
    /* Since SqlTool can run against different versions of HSQLDB (plus
     * against any JDBC database), it can't make assumptions about
     * commands which may cause implicit commits, or commit state
     * requirements with specific databases may have for specific SQL
     * statements.  Therefore, we just assume that any statement other
     * than COMMIT or SET AUTOCOMMIT causes an implicit COMMIT (the
     * Java API spec mandates that setting AUTOCOMMIT causes an implicit
     * COMMIT, regardless of whether turning AUTOCOMMIT on or off).
     */

    private static final String DIVIDER =
        "-----------------------------------------------------------------"
        + "-----------------------------------------------------------------";
    // Needs to be at least as wide as the widest field or header displayed.
    private static String revnum = null;

    static {
        revnum = "$Revision$".substring("$Revision: ".length(),
                "$Revision$".length() - 2);
    }

    private String DSV_OPTIONS_TEXT = null;
    private String D_OPTIONS_TEXT = null;

    /**
     * Legacy wrapper (for before we passed "macros").
     */
    public SqlFile(File file, boolean interactive, Map userVars)
            throws IOException {
        this(file, interactive, userVars, null);
    }

    /**
     * Interpret lines of input file as SQL Statements, Comments,
     * Special Commands, and Buffer Commands.
     * Most Special Commands and many Buffer commands are only for
     * interactive use.
     *
     * @param file  file of null means to read stdin.
     * @param interactive  If true, prompts are printed, the interactive
     *                       Special commands are enabled, and
     *                       continueOnError defaults to true.
     * @throws IOException  If can't open specified SQL file.
     */
    public SqlFile(File file, boolean interactive, Map userVars, Map macros)
            throws IOException {
        logger.privlog(Level.FINER, "<init>ting SqlFile instance",
                null, 2, FrameworkLogger.class);

        // Set up ResourceBundle first, so that any other errors may be
        // reported with localized messages.
        try {
            rb = new SqltoolRB();
            rb.validate();
            rb.setMissingPosValueBehavior(
                    ValidatingResourceBundle.NOOP_BEHAVIOR);
            rb.setMissingPropertyBehavior(
                    ValidatingResourceBundle.NOOP_BEHAVIOR);
            //if (true) throw new RuntimeException("Forced");
        } catch (RuntimeException re) {
            logger.privlog(Level.SEVERE,
                    "Failed to initialize resource bundle", null, 2,
                    FrameworkLogger.class);
            throw re;
        }
        rawPrompt = rb.getString(SqltoolRB.RAWMODE_PROMPT) + "> ";
        DSV_OPTIONS_TEXT = rb.getString(SqltoolRB.DSV_OPTIONS);
        D_OPTIONS_TEXT = rb.getString(SqltoolRB.D_OPTIONS);
        DSV_X_SYNTAX_MSG = rb.getString(SqltoolRB.DSV_X_SYNTAX);
        DSV_M_SYNTAX_MSG = rb.getString(SqltoolRB.DSV_M_SYNTAX);
        nobufferYetString = rb.getString(SqltoolRB.NOBUFFER_YET);

        this.file        = file;
        this.interactive = interactive;
        this.userVars    = userVars;
        this.macros    = macros;
        if (this.userVars == null) {
            this.userVars = new HashMap();
        }
        if (this.macros == null) {
            this.macros = new HashMap();
        }
        updateUserSettings();

        if (file != null &&!file.canRead()) {
            throw new IOException(rb.getString(SqltoolRB.SQLFILE_READFAIL,
                    file.toString()));
        }
        if (interactive) {
            history = new TokenList();
            String histLenString = System.getProperty("sqltool.historyLength");
            if (histLenString != null) try {
                maxHistoryLength = Integer.parseInt(histLenString);
            } catch (Exception e) {
                // what to do, what to do...
            } else {
                maxHistoryLength = DEFAULT_HISTORY_SIZE;
            }
        }
    }


    /**
     * Legacy wrapper (for before we passed "macros").
     */
    public SqlFile(boolean interactive, Map userVars) throws IOException {
        this(null, interactive, userVars, null);
    }

    /**
     * Constructor for reading stdin instead of a file for commands.
     *
     * @see #SqlFile(File, boolean, Map, Map)
     */
    public SqlFile(boolean interactive, Map userVars, Map macros)
            throws IOException {
        this(null, interactive, userVars, macros);
    }

    /**
     * Process all the commands on stdin.
     *
     * @param conn The JDBC connection to use for SQL Commands.
     * @see #execute(Connection, PrintStream, PrintStream, Boolean)
     */
    public void execute(Connection conn,
                        Boolean coeOverride)
                        throws SqlToolError, SQLException {
        execute(conn, System.out, coeOverride);
    }

    /**
     * Process all the commands on stdin.
     *
     * @param conn The JDBC connection to use for SQL Commands.
     * @see #execute(Connection, PrintStream, PrintStream, Boolean)
     */
    public void execute(Connection conn, boolean coeOverride)
                        throws SqlToolError, SQLException {
        execute(conn, System.out, new Boolean(coeOverride));
    }

    public void execute(Connection curConn, PrintStream psStd,
                                     PrintStream psErr,
                                     Boolean coeOverride)
                                     throws SqlToolError,
                                         SQLException {
        psErr.println(rb.getString(SqltoolRB.ERRSTREAM_DEPRECATED));
        execute(curConn, psStd, coeOverride);
    }

    // So we can tell how to handle quit and break commands.
    public boolean      recursed     = false;
    private PrintStream psStd        = null;
    private PrintWriter pwQuery      = null;
    private PrintWriter pwDsv        = null;
    private boolean     continueOnError = false;
    /*
     * This is reset upon each execute() invocation (to true if interactive,
     * false otherwise).
     */
    private static final String DEFAULT_CHARSET = null;
    // Change to Charset.defaultCharset().name(); once we can use Java 1.5!
    private SqlFileScanner      scanner         = null;
    private String              charset         = null;
    private Token               buffer          = null;
    private boolean             preempt         = false;
    private String              lastSqlStatement = null;

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
     * @param curConn The JDBC connection to use for SQL Commands.
     * @throws SQLExceptions thrown by JDBC driver.
     *                       Only possible if in "\c false" mode.
     * @throws SqlToolError  all other errors.
     *               This includes including QuitNow, BreakException,
     *               ContinueException for recursive calls only.
     */
    public void execute(Connection curConn, PrintStream psStd,
                                     Boolean coeOverride)
                                     throws SqlToolError,
                                         SQLException {
        this.curConn = curConn;
        this.psStd = psStd;
        buffer = null;
        continueOnError = (coeOverride == null) ? interactive
                                                : coeOverride.booleanValue();
        String specifiedCharSet = System.getProperty("sqlfile.charset");

        charset = ((specifiedCharSet == null) ? DEFAULT_CHARSET
                                              : specifiedCharSet);
        lastSqlStatement = null;

        // Replace with just "(new FileInputStream(file), charset)"
        // once use defaultCharset from Java 1.5 in charset init. above.
        try {
            scanner = new SqlFileScanner((charset == null)
                    ?  (new InputStreamReader((file == null)
                            ? System.in : (new FileInputStream(file))))
                    :  (new InputStreamReader(((file == null)
                            ? System.in : (new FileInputStream(file))),
                                    charset)));
            scanner.setStdPrintStream(psStd);
            scanner.setResourceBundle(rb);
            if (interactive) {
                stdprintln(rb.getString(SqltoolRB.SQLFILE_BANNER, revnum));
                scanner.setRawPrompt(rawPrompt);
                scanner.setSqlPrompt(contPrompt);
                scanner.setSqltoolPrompt(primaryPrompt);
                scanner.setInteractive(true);
                stdprint(primaryPrompt);
            }
            scanpass(scanner);
        } catch (IOException ioe) {
            throw new SqlToolError(rb.getString(
                    SqltoolRB.PRIMARYINPUT_ACCESSFAIL), ioe);
        } finally {
            if (scanner != null) try {
                scanner.yyclose();
                closeQueryOutputStream();
            } catch (IOException ioe) {
                errprintln("Failed to close pipes");
            }
        }
    }


    /**
     * Returns normalized nesting command String, like "if" or "foreach".
     * If command is not a nesting command, returns null;
     * If there's a proper command String, but the entire PL command is
     * malformatted, throws.
     */
    private String nestingCommand(Token token) throws BadSpecial {
        if (token.type != Token.PL_TYPE) return null;
        // The scanner assures that val is non-null for PL_TYPEs.
        String commandWord = token.val.replaceFirst("\\s.*", "");
        if (!nestingPLCommands.containsKey(commandWord)) return null;
        Pattern pattern = (Pattern) nestingPLCommands.get(commandWord);
        if (pattern.matcher(token.val).matches()) return commandWord;
        throw new BadSpecial(rb.getString(SqltoolRB.PL_MALFORMAT));
    }

    public synchronized void scanpass(TokenSource ts)
                                     throws SqlToolError, SQLException {
        boolean rollbackUncoms = true;
        String nestingCommand;
        Token token = null;

        if (userVars.size() > 0) {
            plMode = true;
        }

        try {
            while (true) try {
                if (preempt) {
                    token = buffer;
                    preempt = false;
                } else {
                    token = ts.yylex();
                    logger.finest("SqlFile got new token:  " + token);
                }
                if (token == null) break;

                nestingCommand = nestingCommand(token);
                if (nestingCommand != null) {
                    if (token.nestedBlock == null) {
                        token.nestedBlock = seekTokenSource(nestingCommand);
                        /* This command (and the same recursive call inside
                         * of the seekTokenSource() method) ensure that all
                         * "blocks" are tokenized immediately as block
                         * commands are encountered, and the blocks are
                         * tokenized in their entirety all the way to the
                         * leaves.
                         */
                    }
                    processBlock(token);
                        /* processBlock recurses through scanpass(),
                         * which processes the nested commands which have
                         * (in all cases) already beeen tokenized.
                         */
                    continue;
                }

                switch (token.type) {
                    case Token.SYNTAX_ERR_TYPE:
                        throw new SqlToolError(rb.getString(
                                SqltoolRB.INPUT_MALFORMAT));
                        // Will get here if Scanner can't match input to any
                        // known command type.
                        // An easy way to get here is to start a command with
                        // quotes.
                    case Token.UNTERM_TYPE:
                        throw new SqlToolError(rb.getString(
                                SqltoolRB.INPUT_UNTERMINATED,
                                        token.val));
                    case Token.RAW_TYPE:
                    case Token.RAWEXEC_TYPE:
                        /*
                         * A real problem in this block is that the Scanner
                         * has already displayed the next prompt at this
                         * point.  We handle this specially within this
                         * block, but if we throw, the handler will not
                         * know that the prompt has to be re-displayed.
                         * I.e., KNOWN ISSUE:  For some errors caught during
                         * raw command execution, interactive users will not
                         * get a prompt to tell them to proceed.
                         */
                        if (token.val == null) token.val = "";
                        /*
                         * Don't have time know to figure out whether it would
                         * ever be useful to send just (non-zero) whitespace
                         * to the DB.  Prohibiting for now.
                         */
                        if (token.val.trim().length() < 1) {
                            throw new SqlToolError(
                                    rb.getString(SqltoolRB.RAW_EMPTY));
                        }
                        int receivedType = token.type;
                        token.type = Token.SQL_TYPE;
                        if (setBuf(token) && receivedType == Token.RAW_TYPE
                                && interactive) {
                            stdprintln("");
                            stdprintln(rb.getString(
                                    SqltoolRB.RAW_MOVEDTOBUFFER));
                            stdprint(primaryPrompt);
                            // All of these stdprint*'s are to work around a
                            // very complicated issue where the Scanner
                            // has already displayed the next prompt before
                            // we can display our status message.
                        }
                        if (receivedType == Token.RAWEXEC_TYPE) {
                            historize();
                            processSQL();
                        }
                        continue;
                    case Token.MACRO_TYPE:
                        processMacro(token);
                        continue;
                    case Token.PL_TYPE:
                        setBuf(token);
                        historize();
                        processPL(null);
                        continue;
                    case Token.SPECIAL_TYPE:
                        setBuf(token);
                        historize();
                        processSpecial(null);
                        continue;
                    case Token.EDIT_TYPE:
                        // Scanner only returns EDIT_TYPEs in interactive mode
                        processBuffHist(token);
                        continue;
                    case Token.BUFFER_TYPE:
                        token.type = Token.SQL_TYPE;
                        if (setBuf(token)) {
                            stdprintln(rb.getString(
                                    SqltoolRB.INPUT_MOVEDTOBUFFER));
                        }
                        continue;
                    case Token.SQL_TYPE:
                        if (token.val == null) token.val = "";
                        setBuf(token);
                        historize();
                        processSQL();
                        continue;
                    default:
                        throw new RuntimeException(
                                "Internal assertion failed.  "
                                + "Unexpected token type: "
                                + token.getTypeString());
                }
            } catch (BadSpecial bs) {
                // BadSpecials ALWAYS have non-null getMessage().
                if (token == null) {
                    errprintln(rb.getString(SqltoolRB.ERRORAT,
                            new String[] {
                                ((file == null) ? "stdin" : file.toString()),
                                "?", "?", bs.getMessage(),
                            }
                    ));
                } else {
                    errprintln(rb.getString(SqltoolRB.ERRORAT,
                            new String[] {
                                ((file == null) ? "stdin" : file.toString()),
                                Integer.toString(token.line),
                                token.reconstitute(),
                                bs.getMessage(),
                            }
                    ));
                }
                Throwable cause = bs.getCause();
                if (cause != null) {
                    errprintln(rb.getString(SqltoolRB.CAUSEREPORT,
                            cause.toString()));

                }

                if (!continueOnError) {
                    throw new SqlToolError(bs);
                }
            } catch (SQLException se) {
                errprintln("SQL " + rb.getString(SqltoolRB.ERRORAT,
                        new String[] {
                            ((file == null) ? "stdin" : file.toString()),
                            ((token == null) ? "?"
                                             : Integer.toString(token.line)),
                            lastSqlStatement,
                            se.getMessage(),
                        }));
                // It's possible that we could have
                // SQLException.getMessage() == null, but if so, I think
                // it reasonable to show "null".  That's a DB inadequacy.

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
                    errprintln(rb.getString(SqltoolRB.BREAK_UNSATISFIED,
                            msg));
                }

                if (recursed ||!continueOnError) {
                    throw be;
                }
            } catch (ContinueException ce) {
                String msg = ce.getMessage();

                if (recursed) {
                    rollbackUncoms = false;
                } else {
                    errprintln(rb.getString(SqltoolRB.CONTINUE_UNSATISFIED,
                            msg));
                }

                if (recursed ||!continueOnError) {
                    throw ce;
                }
            } catch (QuitNow qn) {
                throw qn;
            } catch (SqlToolError ste) {
                StringBuffer sb = new StringBuffer(rb.getString(
                    SqltoolRB.ERRORAT, ((token == null)
                            ? (new String[] {
                                    ((file == null) ? "stdin" : file.toString()),
                                "?", "?",
                                ((ste.getMessage() == null)
                                        ? "" : ste.getMessage())
                              })
                            : (new String[] {
                                    ((file == null) ? "stdin" : file.toString()),
                                Integer.toString(token.line),
                                ((token.val == null) ? "" : token.reconstitute()),
                                ((ste.getMessage() == null)
                                        ? "" : ste.getMessage())
                              }))
                ));
                if (ste.getMessage() != null) sb.append(LS);
                Throwable cause = ste.getCause();
                errprintln((cause == null) ? sb.toString()
                        : rb.getString(SqltoolRB.CAUSEREPORT,
                            cause.toString()));
                if (!continueOnError) {
                    throw ste;
                }
            }

            rollbackUncoms = false;
            // Exiting gracefully, so don't roll back.
        } catch (IOException ioe) {
            throw new SqlToolError(rb.getString(
                    SqltoolRB.PRIMARYINPUT_ACCESSFAIL), ioe);
        } catch (QuitNow qn) {
            if (recursed) {
                throw qn;
                // Will rollback if conditions otherwise require.
                // Otherwise top level will decide based upon qn.getMessage().
            }
            rollbackUncoms = (qn.getMessage() != null);

            if (rollbackUncoms) {
                errprintln(rb.getString(SqltoolRB.ABORTING, qn.getMessage()));
                throw new SqlToolError(qn.getMessage());
            }

            return;
        } finally {
            if (fetchingVar != null) {
                errprintln(rb.getString(SqltoolRB.PLVAR_SET_INCOMPLETE,
                        fetchingVar));
                rollbackUncoms = true;
            }
            if (rollbackUncoms && (!curConn.getAutoCommit())
                    && possiblyUncommitteds.get()) {
                // Nothing to roll back if autocommit is on.
                errprintln(rb.getString(SqltoolRB.ROLLINGBACK));
                curConn.rollback();
                possiblyUncommitteds.set(false);
            }
        }
    }

    /**
     * Utility nested Exception class for internal use only.
     *
     * Do not instantiate with null message.
     */
    static private class BadSpecial extends AppendableException {
        static final long serialVersionUID = 7162440064026570590L;

        BadSpecial(String s) {
            super(s);
            if (s == null)
                throw new RuntimeException(
                        "Must construct BadSpecials with non-null message");
        }
        BadSpecial(String s, Throwable t) {
            super(s, t);
            if (s == null)
                throw new RuntimeException(
                        "Must construct BadSpecials with non-null message");
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
            this(null, t);
        }

        RowError(String s, Throwable t) {
            super(s, t);
        }
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
    private void processBuffHist(Token token)
    throws BadSpecial, SQLException, SqlToolError {
        if (token.val.length() < 1) {
            throw new BadSpecial(rb.getString(SqltoolRB.BUFHIST_UNSPECIFIED));
        }

        // First handle the simple cases where user may not specify a
        // command number.
        char commandChar = token.val.charAt(0);
        String other       = token.val.substring(1);
        if (other.trim().length() == 0) {
            other = null;
        }
        switch (commandChar) {
            case 'l' :
            case 'b' :
                enforce1charBH(other, 'l');
                if (buffer == null) {
                    stdprintln(nobufferYetString);
                } else {
                    stdprintln(rb.getString(SqltoolRB.EDITBUFFER_CONTENTS,
                            buffer.reconstitute()));
                }

                return;

            case 'h' :
                enforce1charBH(other, 'h');
                showHistory();

                return;

            case '?' :
                stdprintln(rb.getString(SqltoolRB.BUFFER_HELP));

                return;
        }

        Integer histNum = null;
        Matcher hm = slashHistoryPattern.matcher(token.val);
        if (hm.matches()) {
            histNum = historySearch(hm.group(1));
            if (histNum == null) {
                stdprintln(rb.getString(SqltoolRB.SUBSTITUTION_NOMATCH));
                return;
            }
        } else {
            hm = historyPattern.matcher(token.val);
            if (!hm.matches()) {
                throw new BadSpecial(rb.getString(SqltoolRB.EDIT_MALFORMAT));
                // Empirically, I find that this pattern always captures two
                // groups.  Unfortunately, there's no way to guarantee that :( .
            }
            histNum = ((hm.group(1) == null || hm.group(1).length() < 1)
                    ? null : new Integer(hm.group(1)));
        }
        if (hm.groupCount() != 2) {
            throw new BadSpecial(rb.getString(SqltoolRB.EDIT_MALFORMAT));
            // Empirically, I find that this pattern always captures two
            // groups.  Unfortunately, there's no way to guarantee that :( .
        }
        commandChar = ((hm.group(2) == null || hm.group(2).length() < 1)
                ? '\0' : hm.group(2).charAt(0));
        other = ((commandChar == '\0') ? null : hm.group(2).substring(1));
        if (other != null && other.length() < 1) other = null;
        Token targetCommand = ((histNum == null)
                ? null : commandFromHistory(histNum.intValue()));
        // Every command below depends upon buffer content.

        switch (commandChar) {
            case '\0' :  // Special token set above.  Just history recall.
                setBuf(targetCommand);
                stdprintln(rb.getString(SqltoolRB.BUFFER_RESTORED,
                        buffer.reconstitute()));
                return;

            case ';' :
                enforce1charBH(other, ';');

                if (targetCommand != null) setBuf(targetCommand);
                if (buffer == null) throw new BadSpecial(
                        rb.getString(SqltoolRB.NOBUFFER_YET));
                stdprintln(rb.getString(SqltoolRB.BUFFER_EXECUTING,
                            buffer.reconstitute()));
                preempt = true;
                return;

            case 'a' :
                if (targetCommand == null) targetCommand = buffer;
                if (targetCommand == null) throw new BadSpecial(
                        rb.getString(SqltoolRB.NOBUFFER_YET));
                boolean doExec = false;

                if (other != null) {
                    if (other.trim().charAt(other.trim().length() - 1) == ';') {
                        other = other.substring(0, other.lastIndexOf(';'));
                        if (other.trim().length() < 1)
                            throw new BadSpecial(
                                    rb.getString(SqltoolRB.APPEND_EMPTY));
                        doExec = true;
                    }
                }
                Token newToken = new Token(targetCommand.type,
                        targetCommand.val, targetCommand.line);
                if (other != null) newToken.val += other;
                setBuf(newToken);
                if (doExec) {
                    stdprintln(rb.getString(SqltoolRB.BUFFER_EXECUTING,
                            buffer.reconstitute()));
                    preempt = true;
                    return;
                }

                if (interactive) scanner.setMagicPrefix(
                        newToken.reconstitute());

                switch (newToken.type) {
                    case Token.SQL_TYPE:
                        scanner.setRequestedState(SqlFileScanner.SQL);
                        break;
                    case Token.SPECIAL_TYPE:
                        scanner.setRequestedState(SqlFileScanner.SPECIAL);
                        break;
                    case Token.PL_TYPE:
                        scanner.setRequestedState(SqlFileScanner.PL);
                        break;
                    default:
                        throw new RuntimeException(
                            "Internal assertion failed.  "
                            + "Appending to unexpected type: "
                            + newToken.getTypeString());
                }
                scanner.setCommandBuffer(newToken.val);

                return;

            case 'w' :
                if (targetCommand == null) targetCommand = buffer;
                if (targetCommand == null) throw new BadSpecial(
                        rb.getString(SqltoolRB.NOBUFFER_YET));
                if (other == null) {
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.DESTFILE_DEMAND));
                }
                String targetFile = dereference(other.trim(), false);
                // Dereference and trim the target file name
                // This is the only case where we dereference a : command.

                PrintWriter pw = null;
                try {
                    pw = new PrintWriter((charset == null)
                            ?  (new OutputStreamWriter(
                                    new FileOutputStream(targetFile, true)))
                            :  (new OutputStreamWriter(
                                    new FileOutputStream(targetFile, true),
                                            charset))
                            // Appendmode so can append to an SQL script.
                    );
                    // Replace with just "(new FileOutputStream(file), charset)"
                    // once use defaultCharset from Java 1.5 in charset init.
                    // above.

                    pw.println(targetCommand.reconstitute(true));
                    pw.flush();
                } catch (Exception e) {
                    throw new BadSpecial(rb.getString(SqltoolRB.FILE_APPENDFAIL,
                            targetFile), e);
                } finally {
                    if (pw != null) pw.close();
                }

                return;

            case 's' :
                boolean modeExecute = false;
                boolean modeGlobal = false;
                if (targetCommand == null) targetCommand = buffer;
                if (targetCommand == null) throw new BadSpecial(
                        rb.getString(SqltoolRB.NOBUFFER_YET));

                try {
                    if (other == null || other.length() < 3) {
                        throw new BadSubst(rb.getString(
                                SqltoolRB.SUBSTITUTION_MALFORMAT));
                    }
                    Matcher m = substitutionPattern.matcher(other);
                    if (!m.matches()) {
                        throw new BadSubst(rb.getString(
                                SqltoolRB.SUBSTITUTION_MALFORMAT));
                    }

                    // Note that this pattern does not include the leading :.
                    if (m.groupCount() < 3 || m.groupCount() > 4) {
                        throw new RuntimeException(
                                "Internal assertion failed.  "
                                + "Matched substitution "
                                + "pattern, but captured "
                                + m.groupCount() + " groups");
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
                            + ')' + m.group(2)).matcher(targetCommand.val);
                    Token newBuffer = new Token(targetCommand.type,
                            (modeGlobal
                                ? bufferMatcher.replaceAll(m.group(3))
                                : bufferMatcher.replaceFirst(m.group(3))),
                                targetCommand.line);
                    if (newBuffer.val.equals(targetCommand.val)) {
                        stdprintln(rb.getString(
                                SqltoolRB.SUBSTITUTION_NOMATCH));
                        return;
                    }

                    setBuf(newBuffer);
                    stdprintln(rb.getString((modeExecute
                            ? SqltoolRB.BUFFER_EXECUTING
                            : SqltoolRB.EDITBUFFER_CONTENTS),
                                buffer.reconstitute()));
                } catch (PatternSyntaxException pse) {
                    throw new BadSpecial(
                            rb.getString(SqltoolRB.SUBSTITUTION_SYNTAX), pse);
                } catch (BadSubst badswitch) {
                    throw new BadSpecial(
                            rb.getString(SqltoolRB.SUBSTITUTION_SYNTAX));
                }
                if (modeExecute) preempt = true;

                return;
        }

        throw new BadSpecial(rb.getString(SqltoolRB.BUFFER_UNKNOWN,
                Character.toString(commandChar)));
    }

    private boolean doPrepare   = false;
    private String  prepareVar  = null;
    private String  dsvColDelim = null;
    private String  dsvColSplitter = null;
    private String  dsvSkipPrefix = null;
    private String  dsvRowDelim = null;
    private String  dsvRowSplitter = null;
    private String  dsvSkipCols = null;
    private boolean dsvTrimAll       = false;
    private String  DSV_X_SYNTAX_MSG = null;
    private String  DSV_M_SYNTAX_MSG = null;
    private String  nobufferYetString = null;

    private void enforce1charSpecial(String tokenString, char command)
            throws BadSpecial {
        if (tokenString.length() != 1) {
            throw new BadSpecial(rb.getString(SqltoolRB.SPECIAL_EXTRACHARS,
                     Character.toString(command), tokenString.substring(1)));
        }
    }
    private void enforce1charBH(String tokenString, char command)
            throws BadSpecial {
        if (tokenString != null) {
            throw new BadSpecial(rb.getString(SqltoolRB.BUFFER_EXTRACHARS,
                    Character.toString(command), tokenString));
        }
    }

    /**
     * Process a Special Command.
     *
     * @param inString TRIMMED, no-null command (without leading \),
     *                 or null to operate on buffer.
     * @throws SQLException thrown by JDBC driver.
     * @throws BadSpecial special-command-specific errors.
     * @throws SqlToolError all other errors, plus QuitNow,
     *                      BreakException, ContinueException.
     */
    private void processSpecial(String inString)
    throws BadSpecial, QuitNow, SQLException, SqlToolError {
        String string = (inString == null) ? buffer.val : inString;
        if (string.length() < 1) {
            throw new BadSpecial(rb.getString(SqltoolRB.SPECIAL_UNSPECIFIED));
        }
        Matcher m = specialPattern.matcher(
                plMode ? dereference(string, false) : string);
        if (!m.matches()) {
            throw new BadSpecial(rb.getString(SqltoolRB.SPECIAL_MALFORMAT));
            // I think it's impossible to get here, since the pattern is
            // so liberal.
        }
        if (m.groupCount() < 1 || m.groupCount() > 2) {
            // Failed assertion
            throw new RuntimeException(
                    "Internal assertion failed.  Pattern matched, yet captured "
                    + m.groupCount() + " groups");
        }

        String arg1 = m.group(1);
        String other = ((m.groupCount() > 1) ? m.group(2) : null);

        switch (arg1.charAt(0)) {
            case 'q' :
                enforce1charSpecial(arg1, 'q');
                if (other != null) {
                    throw new QuitNow(other);
                }

                throw new QuitNow();
            case 'H' :
                enforce1charSpecial(arg1, 'H');
                htmlMode = !htmlMode;

                stdprintln(rb.getString(SqltoolRB.HTML_MODE,
                        Boolean.toString(htmlMode)));

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
                        throw new BadSpecial(rb.getString(
                                    SqltoolRB.DSV_TARGETFILE_DEMAND));
                    }
                    File dsvFile = new File((dsvTargetFile == null)
                                            ? (tableName + ".dsv")
                                            : dsvTargetFile);

                    pwDsv = new PrintWriter((charset == null)
                       ? (new OutputStreamWriter(new FileOutputStream(dsvFile)))
                       : (new OutputStreamWriter(new FileOutputStream(dsvFile),
                               charset)));
                    // Replace with just "(new FileOutputStream(file), charset)"
                    // once use defaultCharset from Java 1.5 in charset init.
                    // above.

                    ResultSet rs = curConn.createStatement().executeQuery(
                            (tableName == null) ? other
                                                : ("SELECT * FROM "
                                                   + tableName));
                    try {
                        List colList = new ArrayList();
                        int[] incCols = null;
                        if (dsvSkipCols != null) {
                            Set skipCols = new HashSet();
                            String[] skipColsArray =
                                    dsvSkipCols.split(dsvColDelim, -1);
                            // Don't know if better to use dsvColDelim or
                            // dsvColSplitter.  Going with former, since the
                            // latter should not need to be set for eXporting
                            // (only importing).
                            for (int i = 0; i < skipColsArray.length; i++) {
                                skipCols.add(skipColsArray[i].trim().toLowerCase());
                            }
                            ResultSetMetaData rsmd = rs.getMetaData();
                            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                                if (!skipCols.remove(rsmd.getColumnName(i)
                                        .toLowerCase())) {
                                    colList.add(new Integer(i));
                                }
                            }
                            if (colList.size() < 1) {
                                throw new BadSpecial(rb.getString(
                                        SqltoolRB.DSV_NOCOLSLEFT, dsvSkipCols));
                            }
                            if (skipCols.size() > 0) {
                                throw new BadSpecial(rb.getString(
                                        SqltoolRB.DSV_SKIPCOLS_MISSING,
                                            skipCols.toString()));
                            }
                            incCols = new int[colList.size()];
                            for (int i = 0; i < incCols.length; i++) {
                                incCols[i] = ((Integer) colList.get(i)).intValue();
                            }
                        }
                        displayResultSet(null, rs, incCols, null);
                    } finally {
                        rs.close();
                    }
                    pwDsv.flush();
                    stdprintln(rb.getString(SqltoolRB.FILE_WROTECHARS,
                            Long.toString(dsvFile.length()),
                            dsvFile.toString()));
                } catch (FileNotFoundException e) {
                    throw new BadSpecial(rb.getString(SqltoolRB.FILE_WRITEFAIL,
                            other), e);
                } catch (UnsupportedEncodingException e) {
                    throw new BadSpecial(rb.getString(SqltoolRB.FILE_WRITEFAIL,
                            other), e);
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
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.METADATA_FETCH_FAIL), se);
                }

                throw new BadSpecial(rb.getString(SqltoolRB.SPECIAL_D_LIKE));
            case 'o' :
                enforce1charSpecial(arg1, 'o');
                if (other == null) {
                    if (pwQuery == null) {
                        throw new BadSpecial(rb.getString(
                                SqltoolRB.OUTPUTFILE_NONETOCLOSE));
                    }

                    closeQueryOutputStream();

                    return;
                }

                if (pwQuery != null) {
                    stdprintln(rb.getString(SqltoolRB.OUTPUTFILE_REOPENING));
                    closeQueryOutputStream();
                }

                try {
                    pwQuery = new PrintWriter((charset == null)
                            ? (new OutputStreamWriter(
                                    new FileOutputStream(other, true)))
                            : (new OutputStreamWriter(
                                    new FileOutputStream(other, true), charset))
                    );
                    // Replace with just "(new FileOutputStream(file), charset)"
                    // once use defaultCharset from Java 1.5 in charset init.
                    // above.

                    /* Opening in append mode, so it's possible that we will
                     * be adding superfluous <HTML> and <BODY> tags.
                     * I think that browsers can handle that */
                    pwQuery.println((htmlMode
                            ? ("<HTML>" + LS + "<!--")
                            : "#") + " " + (new java.util.Date()) + ".  "
                                    + rb.getString(SqltoolRB.OUTPUTFILE_HEADER,
                                            getClass().getName())
                                    + (htmlMode ? (" -->" + LS + LS + "<BODY>")
                                                : LS));
                    pwQuery.flush();
                } catch (Exception e) {
                    throw new BadSpecial(rb.getString(SqltoolRB.FILE_WRITEFAIL,
                            other), e);
                }

                return;

            case 'i' :
                enforce1charSpecial(arg1, 'i');
                if (other == null) {
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.SQLFILE_NAME_DEMAND));
                }

                try {
                    SqlFile sf =
                        new SqlFile(new File(other), false, userVars, macros);

                    sf.recursed = true;
                    // Don't need to unset "recursed", since "sf" will be
                    // out-of-scope after recursion completes.

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
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.SQLFILE_EXECUTE_FAIL, other), e);
                }

                return;

            case 'p' :
                enforce1charSpecial(arg1, 'p');
                if (other == null) {
                    stdprintln(true);
                } else {
                    stdprintln(other, true);
                }

                return;

            case 'l' :
                if ((arg1.equals("l?") && other == null)
                        || (arg1.equals("l") && other != null
                                && other.equals("?"))) {
                    stdprintln(rb.getString(SqltoolRB.LOG_SYNTAX));
                } else {
                    enforce1charSpecial(arg1, 'l');
                    Matcher logMatcher = ((other == null) ? null
                            : logPattern.matcher(other.trim()));
                    if (logMatcher == null || (!logMatcher.matches()))
                        throw new BadSpecial(
                                rb.getString(SqltoolRB.LOG_SYNTAX_ERROR));
                    String levelString = logMatcher.group(1);
                    Level level = null;
                    if (levelString.equalsIgnoreCase("FINER"))
                        level = Level.FINER;
                    else if (levelString.equalsIgnoreCase("WARNING"))
                        level = Level.WARNING;
                    else if (levelString.equalsIgnoreCase("SEVERE"))
                        level = Level.SEVERE;
                    else if (levelString.equalsIgnoreCase("INFO"))
                        level = Level.INFO;
                    else if (levelString.equalsIgnoreCase("FINEST"))
                        level = Level.FINEST;
                    if (level == null)
                        throw new RuntimeException(
                                "Internal assertion failed.  "
                                + " Unexpected Level string: " + levelString);
                    logger.enduserlog(level, logMatcher.group(2));
                }

                return;

            case 'a' :
                enforce1charSpecial(arg1, 'a');
                if (other != null) {
                    curConn.setAutoCommit(
                        Boolean.valueOf(other).booleanValue());
                    possiblyUncommitteds.set(false);
                }

                stdprintln(rb.getString(SqltoolRB.A_SETTING,
                        Boolean.toString(curConn.getAutoCommit())));

                return;
            case 'v' :
                enforce1charSpecial(arg1, 'v');
                if (other != null) {
                    if (integerPattern.matcher(other).matches()) {
                        curConn.setTransactionIsolation(
                                Integer.parseInt(other));
                    } else {
                        RCData.setTI(curConn, other);
                    }
                }

                stdprintln(rb.getString(SqltoolRB.TRANSISO_REPORT,
                        (curConn.isReadOnly() ? "R/O " : "R/W "),
                        RCData.tiToString(curConn.getTransactionIsolation())));

                return;
            case '=' :
                enforce1charSpecial(arg1, '=');
                curConn.commit();
                possiblyUncommitteds.set(false);
                stdprintln(rb.getString(SqltoolRB.COMMITTED));

                return;

            case 'b' :
                if (arg1.length() == 1) {
                    if (other != null) {
                        throw new BadSpecial(rb.getString(
                                SqltoolRB.SPECIAL_B_MALFORMAT));
                    }
                    fetchBinary = true;

                    return;
                }

                if (arg1.charAt(1) == 'p') {
                    if (other != null) {
                        throw new BadSpecial(rb.getString(
                                SqltoolRB.SPECIAL_B_MALFORMAT));
                    }
                    doPrepare = true;

                    return;
                }

                if ((arg1.charAt(1) != 'd' && arg1.charAt(1) != 'l')
                        || other == null) {
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.SPECIAL_B_MALFORMAT));
                }

                File otherFile = new File(other);

                try {
                    if (arg1.charAt(1) == 'd') {
                        dump(otherFile);
                    } else {
                        binBuffer = SqlFile.loadBinary(otherFile);
                        stdprintln(rb.getString(
                                SqltoolRB.BINARY_LOADEDBYTESINTO,
                                        binBuffer.length));
                    }
                } catch (BadSpecial bs) {
                    throw bs;
                } catch (IOException ioe) {
                    throw new BadSpecial(rb.getString(SqltoolRB.BINARY_FILEFAIL,
                            other), ioe);
                }

                return;

            case 't' :
                enforce1charSpecial(arg1, '=');
                if (other != null) {
                    // But remember that we have to abort on some I/O errors.
                    reportTimes = Boolean.valueOf(other).booleanValue();
                }

                stdprintln(rb.getString(
                        SqltoolRB.EXECTIME_REPORTING,
                                Boolean.toString(reportTimes)));

                return;

            case '*' :
            case 'c' :
                enforce1charSpecial(arg1, '=');
                if (other != null) {
                    // But remember that we have to abort on some I/O errors.
                    continueOnError = Boolean.valueOf(other).booleanValue();
                }

                stdprintln(rb.getString(SqltoolRB.C_SETTING,
                        Boolean.toString(continueOnError)));

                return;

            case '?' :
                stdprintln(rb.getString(SqltoolRB.SPECIAL_HELP));

                return;

            case '!' :
                /* N.b. This DOES NOT HANDLE UNIX shell wildcards, since there
                 * is no UNIX shell involved.
                 * Doesn't make sense to incur overhead of a shell without
                 * stdin capability.
                 * Could pipe System.in to the forked process, but that's
                 * probably not worth the effort due to Java's terrible
                 * and inescapable System.in buffering.  I.e., the forked
                 * program or shell wouldn't get stdin until user hits Enter.
                 *
                 * I'd like to execute the user's default shell if they
                 * ran "\!" with no argument, but (a) there is no portable
                 * way to determine the user's default or login shell; and
                 * (b) shell is useless without stdin ability.
                 */

                InputStream stream;
                byte[]      ba         = new byte[1024];
                String      extCommand = ((arg1.length() == 1)
                        ? "" : arg1.substring(1))
                    + ((arg1.length() > 1 && other != null)
                       ? " " : "") + ((other == null) ? "" : other);
                if (extCommand.trim().length() < 1)
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.BANG_INCOMPLETE));

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

                    String s;
                    while ((i = stream.read(ba)) > 0) {
                        s = new String(ba, 0, i);
                        if (s.endsWith(LS)) {
                            // This block just prevents logging of
                            // double-line-breaks.
                            if (s.length() == LS.length()) continue;
                            s = s.substring(0, s.length() - LS.length());
                        }
                        logger.severe(s);
                    }

                    stream.close();

                    if (proc.waitFor() != 0) {
                        throw new BadSpecial(rb.getString(
                                SqltoolRB.BANG_COMMAND_FAIL, extCommand));
                    }
                } catch (BadSpecial bs) {
                    throw bs;
                } catch (Exception e) {
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.BANG_COMMAND_FAIL, extCommand), e);
                }

                return;
        }

        throw new BadSpecial(rb.getString(SqltoolRB.SPECIAL_UNKNOWN,
                Character.toString(arg1.charAt(0))));
    }

    private static final char[] nonVarChars = {
        ' ', '\t', '=', '}', '\n', '\r', '\f'
    };

    /**
     * Returns index specifying 1 past end of a variable name.
     *
     * @param inString String containing a variable name
     * @param startIndex Index within inString where the variable name begins
     * @return Index within inString, 1 past end of the variable name
     */
    static int pastName(String inString, int startIndex) {
        String workString = inString.substring(startIndex);
        int    e          = inString.length();  // Index 1 past end of var name.
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
     * Deference *{} PL variables and ${} System Property variables.
     *
     * @throws SqlToolError
     */
    private String dereference(String inString,
                               boolean permitAlias) throws SqlToolError {
        /* TODO:  Rewrite using java.util.regex. */
        String       varName, varValue;
        StringBuffer expandBuffer = new StringBuffer(inString);
        int          b, e;    // begin and end of name.  end really 1 PAST name
        int iterations;

        if (permitAlias && inString.trim().charAt(0) == '/') {
            int slashIndex = inString.indexOf('/');

            e = SqlFile.pastName(inString.substring(slashIndex + 1), 0);

            // In this case, e is the exact length of the var name.
            if (e < 1) {
                throw new SqlToolError(rb.getString(
                        SqltoolRB.PLALIAS_MALFORMAT));
            }

            varName  = inString.substring(slashIndex + 1, slashIndex + 1 + e);
            varValue = (String) userVars.get(varName);

            if (varValue == null) {
                throw new SqlToolError(rb.getString(
                        SqltoolRB.PLVAR_UNDEFINED, varName));
            }

            expandBuffer.replace(slashIndex, slashIndex + 1 + e,
                                 (String) userVars.get(varName));
        }

        String s;
        boolean permitUnset;
        // Permit unset with:     ${:varname}
        // Prohibit unset with :  ${varnam}

        iterations = 0;
        while (true) {
            s = expandBuffer.toString();
            b = s.indexOf("${");

            if (b < 0) {
                // No more unexpanded variable uses
                break;
            }

            e = s.indexOf('}', b + 2);

            if (e == b + 2) {
                throw new SqlToolError(rb.getString(SqltoolRB.SYSPROP_EMPTY));
            }

            if (e < 0) {
                throw new SqlToolError(rb.getString(
                            SqltoolRB.SYSPROP_UNTERMINATED));
            }

            permitUnset = (s.charAt(b + 2) == ':');

            varName = s.substring(b + (permitUnset ? 3 : 2), e);
            if (iterations++ > 10000)
                throw new SqlToolError(rb.getString(SqltoolRB.VAR_INFINITE,
                        varName));

            varValue = System.getProperty(varName);
            if (varValue == null) {
                if (permitUnset) {
                    varValue = "";
                } else {
                    throw new SqlToolError(rb.getString(
                            SqltoolRB.SYSPROP_UNDEFINED, varName));
                }
            }

            expandBuffer.replace(b, e + 1, varValue);
        }

        iterations = 0;
        while (true) {
            s = expandBuffer.toString();
            b = s.indexOf("*{");

            if (b < 0) {
                // No more unexpanded variable uses
                break;
            }

            e = s.indexOf('}', b + 2);

            if (e == b + 2) {
                throw new SqlToolError(rb.getString(SqltoolRB.PLVAR_NAMEEMPTY));
            }

            if (e < 0) {
                throw new SqlToolError(rb.getString(
                            SqltoolRB.PLVAR_UNTERMINATED));
            }

            permitUnset = (s.charAt(b + 2) == ':');

            varName = s.substring(b + (permitUnset ? 3 : 2), e);
            if (iterations++ > 10000)
                throw new SqlToolError(rb.getString(SqltoolRB.VAR_INFINITE,
                        varName));
            // TODO:  Use a smarter algorithm to handle (or prohibit)
            // recursion without this clumsy detection tactic.

            varValue = (String) userVars.get(varName);
            if (varValue == null) {
                if (permitUnset) {
                    varValue = "";
                } else {
                    throw new SqlToolError(rb.getString(
                            SqltoolRB.PLVAR_UNDEFINED, varName));
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
     * Process a block PL command like "if" of "foreach".
     */
    private void processBlock(Token token) throws BadSpecial, SqlToolError {
        Matcher m = plPattern.matcher(dereference(token.val, false));
        if (!m.matches()) {
            throw new BadSpecial(rb.getString(SqltoolRB.PL_MALFORMAT));
            // I think it's impossible to get here, since the pattern is
            // so liberal.
        }
        if (m.groupCount() < 1 || m.group(1) == null) {
            plMode = true;
            stdprintln(rb.getString(SqltoolRB.PL_EXPANSIONMODE, "on"));
            return;
        }

        String[] tokens = m.group(1).split("\\s+", -1);

        // If user runs any PL command, we turn PL mode on.
        plMode = true;

        if (tokens[0].equals("foreach")) {
            Matcher foreachM = foreachPattern.matcher(
                    dereference(token.val, false));
            if (!foreachM.matches()) {
                throw new BadSpecial(rb.getString(SqltoolRB.FOREACH_MALFORMAT));
            }
            if (foreachM.groupCount() != 2) {
                throw new RuntimeException(
                        "Internal assertion failed.  "
                        + "foreach pattern matched, but captured "
                        + foreachM.groupCount() + " groups");
            }

            String varName   = foreachM.group(1);
            if (varName.indexOf(':') > -1) {
                throw new BadSpecial(rb.getString(SqltoolRB.PLVAR_NOCOLON));
            }
            String[] values = foreachM.group(2).split("\\s+", -1);

            String origval = (String) userVars.get(varName);


            try {
                for (int i = 0; i < values.length; i++) {
                    try {
                        userVars.put(varName, values[i]);
                        updateUserSettings();

                        boolean origRecursed = recursed;
                        recursed = true;
                        try {
                            scanpass(token.nestedBlock.dup());
                        } finally {
                            recursed = origRecursed;
                        }
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
            } catch (RuntimeException re) {
                throw re;  // Unrecoverable
            } catch (Exception e) {
                throw new BadSpecial(rb.getString(SqltoolRB.PL_BLOCK_FAIL), e);
            }

            if (origval == null) {
                userVars.remove(varName);
                updateUserSettings();
            } else {
                userVars.put(varName, origval);
            }

            return;
        }

        if (tokens[0].equals("if") || tokens[0].equals("while")) {
            Matcher ifwhileM= ifwhilePattern.matcher(
                    dereference(token.val, false));
            if (!ifwhileM.matches()) {
                throw new BadSpecial(rb.getString(SqltoolRB.IFWHILE_MALFORMAT));
            }
            if (ifwhileM.groupCount() != 1) {
                throw new RuntimeException(
                        "Internal assertion failed.  "
                        + "if/while pattern matched, but captured "
                        + ifwhileM.groupCount() + " groups");
            }

            String[] values =
                    ifwhileM.group(1).replaceAll("!([a-zA-Z0-9*])", "! $1").
                        replaceAll("([a-zA-Z0-9*])!", "$1 !").split("\\s+", -1);

            if (tokens[0].equals("if")) {
                try {
                    if (eval(values)) {
                        boolean origRecursed = recursed;
                        recursed = true;
                        try {
                            scanpass(token.nestedBlock.dup());
                        } finally {
                            recursed = origRecursed;
                        }
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
                    bs.appendMessage(rb.getString(SqltoolRB.IF_MALFORMAT));
                    throw bs;
                } catch (RuntimeException re) {
                    throw re;  // Unrecoverable
                } catch (Exception e) {
                    throw new BadSpecial(
                        rb.getString(SqltoolRB.PL_BLOCK_FAIL), e);
                }
            } else if (tokens[0].equals("while")) {
                try {

                    while (eval(values)) {
                        try {
                            boolean origRecursed = recursed;
                            recursed = true;
                            try {
                                scanpass(token.nestedBlock.dup());
                            } finally {
                                recursed = origRecursed;
                            }
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
                    bs.appendMessage(rb.getString(SqltoolRB.WHILE_MALFORMAT));
                    throw bs;
                } catch (RuntimeException re) {
                    throw re;  // Unrecoverable
                } catch (Exception e) {
                    throw new BadSpecial(rb.getString(SqltoolRB.PL_BLOCK_FAIL),
                            e);
                }
            } else {
                // Assertion
                throw new RuntimeException(rb.getString(SqltoolRB.PL_UNKNOWN,
                        tokens[0]));
            }

            return;
        }

        throw new BadSpecial(rb.getString(SqltoolRB.PL_UNKNOWN, tokens[0]));
    }

    /**
     * Process a Non-Block Process Language Command.
     * Nesting not supported yet.
     *
     * @param inString  Trimmed non-null command without leading *
     *                  (may be empty string "").
     * @throws BadSpecial special-command-specific errors.
     * @throws SqlToolError all other errors, plus BreakException and
     *                      ContinueException.
     */
    private void processPL(String inString) throws BadSpecial, SqlToolError {
        String string = (inString == null) ? buffer.val : inString;
        Matcher m = plPattern.matcher(dereference(string, false));
        if (!m.matches()) {
            throw new BadSpecial(rb.getString(SqltoolRB.PL_MALFORMAT));
            // I think it's impossible to get here, since the pattern is
            // so liberal.
        }
        if (m.groupCount() < 1 || m.group(1) == null) {
            plMode = true;
            stdprintln(rb.getString(SqltoolRB.PL_EXPANSIONMODE, "on"));
            return;
        }

        String[] tokens = m.group(1).split("\\s+", -1);

        if (tokens[0].charAt(0) == '?') {
            stdprintln(rb.getString(SqltoolRB.PL_HELP));

            return;
        }

        // If user runs any PL command, we turn PL mode on.
        plMode = true;

        if (tokens[0].equals("end")) {
            throw new BadSpecial(rb.getString(SqltoolRB.END_NOBLOCK));
        }

        if (tokens[0].equals("continue")) {
            if (tokens.length > 1) {
                if (tokens.length == 2 &&
                        (tokens[1].equals("foreach") ||
                         tokens[1].equals("while"))) {
                    throw new ContinueException(tokens[1]);
                }
                throw new BadSpecial(rb.getString(SqltoolRB.CONTINUE_SYNTAX));
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
                throw new BadSpecial(rb.getString(SqltoolRB.BREAK_SYNTAX));
            }

            throw new BreakException();
        }

        if (tokens[0].equals("list") || tokens[0].equals("listvalues")
                || tokens[0].equals("listsysprops")) {
            boolean sysProps =tokens[0].equals("listsysprops");
            String  s;
            boolean doValues = (tokens[0].equals("listvalues") || sysProps);
            // Always list System Property values.
            // They are unlikely to be very long, like PL variables may be.

            if (tokens.length == 1) {
                stdprint(formatNicely(
                        (sysProps ? System.getProperties() : userVars),
                        doValues));
            } else {
                if (doValues) {
                    stdprintln(rb.getString(SqltoolRB.PL_LIST_PARENS));
                } else {
                    stdprintln(rb.getString(SqltoolRB.PL_LIST_LENGTHS));
                }

                for (int i = 1; i < tokens.length; i++) {
                    s = (String) (sysProps ? System.getProperties() : userVars).
                            get(tokens[i]);
                    if (s == null) continue;
                    stdprintln("    " + tokens[i] + ": "
                               + (doValues ? ("(" + s + ')')
                                           : Integer.toString(s.length())));
                }
            }

            return;
        }

        if (tokens[0].equals("dump") || tokens[0].equals("load")) {
            if (tokens.length != 3) {
                throw new BadSpecial(rb.getString(
                        SqltoolRB.DUMPLOAD_MALFORMAT));
            }

            String varName = tokens[1];

            if (varName.indexOf(':') > -1) {
                throw new BadSpecial(rb.getString(SqltoolRB.PLVAR_NOCOLON));
            }
            File   dlFile    = new File(tokens[2]);

            try {
                if (tokens[0].equals("dump")) {
                    dump(varName, dlFile);
                } else {
                    load(varName, dlFile, charset);
                }
            } catch (IOException ioe) {
                throw new BadSpecial(rb.getString(SqltoolRB.DUMPLOAD_FAIL,
                        varName, dlFile.toString()), ioe);
            }

            return;
        }

        if (tokens[0].equals("prepare")) {
            if (tokens.length != 2) {
                throw new BadSpecial(rb.getString(SqltoolRB.PREPARE_MALFORMAT));
            }

            if (userVars.get(tokens[1]) == null) {
                throw new BadSpecial(rb.getString(
                    SqltoolRB.PLVAR_UNDEFINED, tokens[1]));
            }

            prepareVar = tokens[1];
            doPrepare  = true;

            return;
        }

        m = varsetPattern.matcher(dereference(string, false));
        if (!m.matches()) {
            throw new BadSpecial(rb.getString(SqltoolRB.PL_UNKNOWN, tokens[0]));
        }
        if (m.groupCount() < 2 || m.groupCount() > 3) {
            // Assertion
            throw new RuntimeException("varset pattern matched but captured "
                    + m.groupCount() + " groups");
        }

        String varName  = m.group(1);

        if (varName.indexOf(':') > -1) {
            throw new BadSpecial(rb.getString(SqltoolRB.PLVAR_NOCOLON));
        }

        switch (m.group(2).charAt(0)) {
            case '_' :
                silentFetch = true;
            case '~' :
                if (m.groupCount() > 2 && m.group(3) != null) {
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.PLVAR_TILDEDASH_NOMOREARGS, m.group(3)));
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
                } else {
                    userVars.remove(varName);
                }
                updateUserSettings();

                return;
        }

        throw new BadSpecial(rb.getString(SqltoolRB.PL_UNKNOWN, tokens[0]));
        // I think this would already be caught in the setvar block above.
    }

    /**
     * Wrapper methods so don't need to call x(..., false) in most cases.
     */
    /* Unused.  Enable when/if need.
    private void stdprintln() {
        stdprintln(false);
    }
    */

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
    private void errprintln(String s) {
        if (htmlMode) {
            psStd.println("<DIV style='color:white; background: red; "
                       + "font-weight: bold'>" + s + "</DIV>");
        } else {
            logger.privlog(Level.SEVERE, s, null, 6, SqlFile.class);
            /* Only consistent way we can log source location is to log
             * the caller of SqlFile.
             * This seems acceptable, since the location being reported
             * here is not the source of the problem anyways.  */
        }
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
    private static final String DEFAULT_ROW_DELIM = LS;
    private static final String DEFAULT_ROW_SPLITTER = "\\r\\n|\\r|\\n";
    private static final String DEFAULT_COL_DELIM = "|";
    private static final String DEFAULT_COL_SPLITTER = "\\|";
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
     * Filter handling is admittedly inconsistent, both wrt pattern
     * matching (java.util.regex vs. DB-implemented matching) and
     * which columns the filter is matched against.
     * The former is because, for performance and because the DB should
     * know best how to supply the desired results, we need to let the
     * database do filtering if at all possible.
     * In many cases, the DB does not have a filter option, so we have
     * to filter ourselves.
     * For the latter, we have no control over which columsn the DB
     * matches agains, plus the displayResultSet() method in this class
     * can only match against all columns (only reason not to add
     * column-specific filtering is to keep the complexity manageable).
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
                        errprintln(rb.getString(SqltoolRB.VENDOR_ORACLE_DS));

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
                        if (filter != null) {
                            Matcher matcher = nameDotPattern.matcher(filter);
                            if (matcher.matches()) {
                                narrower = "\nWHERE sequence_schema = '"
                                        + matcher.group(1) + "'";
                                filter = null;
                            }
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
                            rb.getString(SqltoolRB.VENDOR_DERBY_DR));
                    } else {
                        throw new BadSpecial(
                            rb.getString(SqltoolRB.VENDOR_NOSUP_D, "r"));
                    }
                    break;

                case 'u' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT user_name, admin FROM "
                            + "information_schema.system_users\n"
                            + "ORDER BY user_name");
                        // Was "user" instead of "username" before HSQLDB 1.9.
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
                            rb.getString(SqltoolRB.VENDOR_DERBY_DU));
                    } else {
                        throw new BadSpecial(
                            rb.getString(SqltoolRB.VENDOR_NOSUP_D, "u"));
                    }
                    break;

                case 'a' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        //  HSQLDB Aliases are not the same things as the
                        //  aliases listed in DatabaseMetaData.getTables().
                        if (filter != null) {
                            Matcher matcher = nameDotPattern.matcher(filter);
                            if (matcher.matches()) {
                                narrower = "\nWHERE alias_schema = '"
                                    + matcher.group(1) + "'";
                                filter = null;
                            }
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

                        if (!nameDotPattern.matcher(filter).matches()) {
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
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.SPECIAL_D_UNKNOWN,
                                Character.toString(c)) + LS + D_OPTIONS_TEXT);
            }

            if (statement == null) {
                if (dbProductName.indexOf("HSQL") > -1) {
                    listSet = listMDTableCols[HSQLDB_ELEMENT];
                } else if (dbProductName.indexOf("Oracle") > -1) {
                    listSet = listMDTableCols[ORACLE_ELEMENT];
                } else {
                    listSet = listMDTableCols[DEFAULT_ELEMENT];
                }


                if (schema == null && filter != null) {
                    Matcher matcher = nameDotPattern.matcher(filter);
                    if (matcher.matches()) {
                        schema = matcher.group(1);
                        filter = null;
                    }
                }
            }

            rs = ((statement == null)
                  ? md.getTables(null, schema, null, types)
                  : statement.getResultSet());

            if (rs == null) {
                throw new BadSpecial(rb.getString(
                        SqltoolRB.METADATA_FETCH_FAIL));
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
                        throw new BadSpecial(rb.getString(
                                SqltoolRB.METADATA_FETCH_FAILFOR,
                                        additionalSchemas[i]));
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
            throw new BadSpecial(rb.getString( SqltoolRB.METADATA_FETCH_FAIL),
                    se);
        } catch (NullPointerException npe) {
            throw new BadSpecial(rb.getString( SqltoolRB.METADATA_FETCH_FAIL),
                    npe);
        } finally {
            excludeSysSchemas = false;

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException se) {
                    // We already got what we want from it, or have/are
                    // processing a more specific error.
                }
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException se) {
                    // Purposefully doing nothing
                }
            }
        }
    }

    private boolean excludeSysSchemas = false;

    /**
     * Process the contents of Edit Buffer as an SQL Statement
     *
     * @throws SQLException thrown by JDBC driver.
     * @throws SqlToolError all other errors.
     */
    private void processSQL() throws SQLException, SqlToolError {
        if (buffer == null)
            throw new RuntimeException(
                    "Internal assertion failed.  No buffer in processSQL().");
        if (buffer.type != Token.SQL_TYPE)
            throw new RuntimeException(
                    "Internal assertion failed.  "
                    + "Token type " + buffer.getTypeString()
                    + " in processSQL().");
        // No reason to check autoCommit constantly.  If we need to roll
        // back, we will check the autocommit state at that time.
        lastSqlStatement    = (plMode ? dereference(buffer.val, true)
                                      : buffer.val);
        // N.b. "lastSqlStatement" is a misnomer only inside this method.
        // Outside of this method, this var references the "last" SQL
        // statement which we attempted to execute.
        if ((!permitEmptySqlStatements) && buffer.val == null
                || buffer.val.trim().length() < 1) {
            throw new SqlToolError(rb.getString(SqltoolRB.SQLSTATEMENT_EMPTY));
            // There is nothing inherently wrong with issuing
            // an empty command, like to test DB server health.
            // But, this check effectively catches many syntax
            // errors early.
        }
        Statement statement = null;

        long startTime = 0;
        if (reportTimes) startTime = (new java.util.Date()).getTime();
        try { // VERY outer block just to ensure we close "statement"
        try { if (doPrepare) {
            if (lastSqlStatement.indexOf('?') < 1) {
                lastSqlStatement = null;
                throw new SqlToolError(rb.getString(
                        SqltoolRB.PREPARE_DEMANDQM));
            }

            doPrepare = false;

            PreparedStatement ps = curConn.prepareStatement(lastSqlStatement);
            statement = ps;

            if (prepareVar == null) {
                if (binBuffer == null) {
                    lastSqlStatement = null;
                    throw new SqlToolError(rb.getString(
                            SqltoolRB.BINBUFFER_EMPTY));
                }

                ps.setBytes(1, binBuffer);
            } else {
                String val = (String) userVars.get(prepareVar);

                if (val == null) {
                    lastSqlStatement = null;
                    throw new SqlToolError(
                            rb.getString(SqltoolRB.PLVAR_UNDEFINED,
                                    prepareVar));
                }

                prepareVar = null;

                ps.setString(1, val);
            }

            ps.executeUpdate();
        } else {
            statement = curConn.createStatement();

            statement.execute(lastSqlStatement);
        } } finally {
            if (reportTimes) {
                long elapsed = (new java.util.Date().getTime()) - startTime;
                //condlPrintln("</TABLE>", true);
                condlPrintln(rb.getString(
                        SqltoolRB.EXECTIME_REPORT, (int) elapsed), false);
            }
        }

        /* This catches about the only very safe way to know a COMMIT
         * is not needed. */
        possiblyUncommitteds.set(
            !commitOccursPattern.matcher(lastSqlStatement).matches());

        ResultSet rs = null;
        try {
            rs = statement.getResultSet();
            displayResultSet(statement, rs, null, null);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException se) {
                    // We already got what we want from it, or have/are
                    // processing a more specific error.
                }
            }
        }
        } finally {
            try {
                if (statement != null) statement.close();
            } catch (SQLException se) {
                // Purposefully doing nothing
            }
        }
        lastSqlStatement = null;
    }

    /**
     * Display the given result set for user.
     * The last 3 params are to narrow down records and columns where
     * that can not be done with a where clause (like in metadata queries).
     * <P/>
     * Caller is responsible for closing any passed Statement or ResultSet.
     *
     * @param statement The SQL Statement that the result set is for.
     *                  (This is so we can get the statement's update count.
     *                  Can be null for non-update queries.)
     * @param r         The ResultSet to display.
     * @param incCols   Optional list of which columns to include (i.e., if
     *                  given, then other columns will be skipped).
     * @param filterRegex Optional filter.  Rows are skipped which to not
     *                  contain this substring in ANY COLUMN.
     *                  (Should add another param to specify targeted columns).
     * @throws SQLException thrown by JDBC driver.
     * @throws SqlToolError all other errors.
     */
    private void displayResultSet(Statement statement, ResultSet r,
                                  int[] incCols,
                                  String filterString) throws SQLException,
                                  SqlToolError {
        java.sql.Timestamp ts;
        int dotAt;
        int                updateCount = (statement == null) ? -1
                                                             : statement
                                                                 .getUpdateCount();
        boolean            silent      = silentFetch;
        boolean            binary      = fetchBinary;
        Pattern            filter = null;

        silentFetch = false;
        fetchBinary = false;

        if (filterString != null) try {
            filter = Pattern.compile(filterString);
        } catch (PatternSyntaxException pse) {
            throw new SqlToolError(rb.getString(SqltoolRB.REGEX_MALFORMAT,
                    pse.getMessage()));
        }

        if (excludeSysSchemas) {
            stdprintln(rb.getString(SqltoolRB.VENDOR_NOSUP_SYSSCHEMAS));
        }

        switch (updateCount) {
            case -1 :
                if (r == null) {
                    stdprintln(rb.getString(SqltoolRB.NORESULT), true);

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
                boolean           isValNull;

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

                    if (headerArray[insi] != null
                            && headerArray[insi].length() > maxWidth[insi]) {
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
                        isValNull = true;

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
                                case org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE:
                                case org.hsqldb.types.Types.SQL_TIME_WITH_TIME_ZONE:
                                case java.sql.Types.TIMESTAMP:
                                case java.sql.Types.DATE:
                                case java.sql.Types.TIME:
                                    ts  = r.getTimestamp(i);
                                    isValNull = r.wasNull();
                                    val = ((ts == null) ? null : ts.toString());
                                    // Following block truncates non-zero
                                    // sub-seconds from time types OTHER than
                                    // TIMESTAMP.
                                    if (dataType[insi]
                                            != java.sql.Types.TIMESTAMP
                                            && dataType[insi]
                                            != org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE
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
                                    isValNull = r.wasNull();

                                    // If we tried to get a String but it
                                    // failed, try getting it with a String
                                    // Stream
                                    if (val == null) {
                                        try {
                                            val = streamToString(
                                                r.getAsciiStream(i), charset);
                                            isValNull = r.wasNull();
                                        } catch (Exception e) {
                                            // This isn't an error.
                                            // We are attempting to do a stream
                                            // fetch if-and-only-if the column
                                            // supports it.
                                        }
                                    }
                            }
                        }

                        if (binary || (val == null &&!isValNull)) {
                            if (pwDsv != null) {
                                throw new SqlToolError(
                                        rb.getString(SqltoolRB.DSV_BINCOL));
                            }

                            // DB has a value but we either explicitly want
                            // it as binary, or we failed to get it as String.
                            try {
                                binBuffer =
                                    SqlFile.streamToBytes(r.getBinaryStream(i));
                                isValNull = r.wasNull();
                            } catch (IOException ioe) {
                                throw new SqlToolError(
                                    "Failed to read value using stream",
                                    ioe);
                            }

                            stdprintln(rb.getString(SqltoolRB.BINBUF_WRITE,
                                       Integer.toString(binBuffer.length),
                                       headerArray[insi],
                                       SqlFile.sqlTypeToString(dataType[insi])
                                    ));

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

                        userVars.put("?", ((val == null) ? nullRepToken : val));
                        if (fetchingVar != null) {
                            userVars.put(fetchingVar, userVars.get("?"));
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
                            && (val == null || filter.matcher(val).find())) {
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
                            condlPrint(((i > 0) ? "  " : "")
                                    + ((i < headerArray.length - 1
                                        || rightJust[i])
                                       ? StringUtil.toPaddedString(
                                         headerArray[i], maxWidth[i],
                                         ' ', !rightJust[i])
                                       : headerArray[i])
                                    , false);
                        }

                        condlPrintln(LS + PRE_TR + "</TR>", true);
                        condlPrintln("", false);

                        if (!htmlMode) {
                            for (int i = 0; i < headerArray.length; i++) {
                                condlPrint(((i > 0) ? "  "
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
                            condlPrint(((j > 0) ? "  " : "")
                                    + ((j < fieldArray.length - 1
                                        || rightJust[j])
                                       ? StringUtil.toPaddedString(
                                         fieldArray[j], maxWidth[j],
                                         ' ', !rightJust[j])
                                       : fieldArray[j])
                                    , false);
                        }

                        condlPrintln(LS + PRE_TR + "</TR>", true);
                        condlPrintln("", false);
                    }

                    condlPrintln("</TABLE>", true);

                    if (rows.size() != 1) {
                        stdprintln(LS + rb.getString(SqltoolRB.ROWS_FETCHED,
                                rows.size()), true);
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

                stdprintln(rb.getString(SqltoolRB.ROWS_FETCHED_DSV,
                        rows.size()));
                break;

            default :
                userVars.put("?", Integer.toString(updateCount));
                if (fetchingVar != null) {
                    userVars.put(fetchingVar, userVars.get("?"));
                    updateUserSettings();
                    fetchingVar = null;
                }

                if (updateCount != 0) {
                    stdprintln((updateCount == 1)
                        ? rb.getString(SqltoolRB.ROW_UPDATE_SINGULAR)
                        : rb.getString(SqltoolRB.ROW_UPDATE_MULTIPLE,
                                updateCount));
                }
                break;
        }
    }

    private static final int    COL_HEAD = 0,
                                COL_ODD  = 1,
                                COL_EVEN = 2
    ;
    private static final String PRE_TR   = "    ";
    private static final String PRE_TD   = "        ";

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
     * Display command history.
     */
    private void showHistory() throws BadSpecial {
        if (history == null) {
            throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_UNAVAILABLE));
        }
        if (history.size() < 1) {
            throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_NONE));
        }
        Token token;
        for (int i = 0; i < history.size(); i++) {
            token = (Token) history.get(i);
            psStd.println("#" + (i + oldestHist) + " or "
                    + (i - history.size()) + ':');
            psStd.println(token.reconstitute());
        }
        if (buffer != null) {
            psStd.println(rb.getString(SqltoolRB.EDITBUFFER_CONTENTS,
                    buffer.reconstitute()));
        }

        psStd.println();
        psStd.println(rb.getString(SqltoolRB.BUFFER_INSTRUCTIONS));
    }

    /**
     * Return a Command from command history.
     */
    private Token commandFromHistory(int inIndex) throws BadSpecial {
        int index = inIndex;  // Just to quiet compiler warnings.

        if (history == null) {
            throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_UNAVAILABLE));
        }
        if (index == 0) {
            throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_NUMBER_REQ));
        }
        if (index > 0) {
            // Positive command# given
            index -= oldestHist;
            if (index < 0) {
                throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_BACKTO,
                       oldestHist));
            }
            if (index >= history.size()) {
                throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_UPTO,
                       history.size() + oldestHist - 1));
            }
        } else {
            // Negative command# given
            index += history.size();
            if (index < 0) {
                throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_BACK,
                       history.size()));
            }
        }
        return (Token) history.get(index);
    }

    /**
     * Search Command History for a regex match.
     *
     * @return Absolute command number, if any match.
     */
    private Integer historySearch(String findRegex) throws BadSpecial {
        if (history == null) {
            throw new BadSpecial(rb.getString(SqltoolRB.HISTORY_UNAVAILABLE));
        }
        Pattern pattern = null;
        try {
            pattern = Pattern.compile("(?ims)" + findRegex);
        } catch (PatternSyntaxException pse) {
            throw new BadSpecial(rb.getString(SqltoolRB.REGEX_MALFORMAT,
                    pse.getMessage()));
        }
        // Make matching more liberal.  Users can customize search behavior
        // by using "(?-OPTIONS)" or (?OPTIONS) in their regexes.
        for (int index = history.size() - 1; index >= 0; index--)
            if (pattern.matcher(((Token) history.get(index)).val).find())
                return new Integer(index + oldestHist);
        return null;
    }

    /**
     * Set buffer, unless the given token equals what is already in the
     * buffer.
     */
    private boolean setBuf(Token newBuffer) {
        if (buffer != null)
        if (buffer != null && buffer.equals(newBuffer)) return false;
        switch (newBuffer.type) {
            case Token.SQL_TYPE:
            case Token.PL_TYPE:
            case Token.SPECIAL_TYPE:
                break;
            default:
                throw new RuntimeException(
                        "Internal assertion failed.  "
                        + "Attempted to add command type "
                        + newBuffer.getTypeString() + " to buffer");
        }
        buffer = new Token(newBuffer.type, new String(newBuffer.val),
                newBuffer.line);
        // System.err.println("Buffer is now (" + buffer + ')');
        return true;
    }

    int oldestHist = 1;

    /**
     * Add a command onto the history list.
     */
    private boolean historize() {
        if (history == null || buffer == null) {
            return false;
        }
        if (history.size() > 0 &&
                history.get(history.size() - 1).equals(buffer)) {
            // Don't store two consecutive commands that are exactly the same.
            return false;
        }
        history.add(buffer);
        if (history.size() <= maxHistoryLength) {
            return true;
        }
        history.remove(0);
        oldestHist++;
        return true;
    }

    /**
     * Describe the columns of specified table.
     *
     * @param tableName  Table that will be described.
     * @param filter  Optional regex to filter by.
     *                By default, will match only against the column name.
     *                Prefix with "/" to match against the entire output line.
     */
    private void describe(String tableName,
                          String filterString) throws SQLException {
        /*
         * Doing case-sensitive filters now, for greater portability.
        String filter = ((inFilter == null) ? null : inFilter.toUpperCase());
         */
        Pattern   filter = null;
        boolean   filterMatchesAll = false;  // match filter against all cols.
        List      rows        = new ArrayList();
        String[]  headerArray = {
            rb.getString(SqltoolRB.DESCRIBE_TABLE_NAME),
            rb.getString(SqltoolRB.DESCRIBE_TABLE_DATATYPE),
            rb.getString(SqltoolRB.DESCRIBE_TABLE_WIDTH),
            rb.getString(SqltoolRB.DESCRIBE_TABLE_NONULLS),
        };
        String[]  fieldArray;
        int[]     maxWidth  = {
            0, 0, 0, 0
        };
        boolean[] rightJust = {
            false, false, true, false
        };

        if (filterString != null) try {
            filterMatchesAll = (filterString.charAt(0) == '/');
            filter = Pattern.compile(filterMatchesAll
                    ? filterString.substring(1) : filterString);
        } catch (PatternSyntaxException pse) {
            throw new SQLException(rb.getString(SqltoolRB.REGEX_MALFORMAT,
                    pse.getMessage()));
            // This is obviously not a SQLException.
            // Perhaps change input parameter to a Pattern to require
            // caller to compile the pattern?
        }

        for (int i = 0; i < headerArray.length; i++) {
            if (htmlMode) {
                continue;
            }

            if (headerArray[i].length() > maxWidth[i]) {
                maxWidth[i] = headerArray[i].length();
            }
        }

        ResultSet r         = null;
        Statement statement = curConn.createStatement();

        // STEP 1: GATHER DATA
        try {
            statement.execute("SELECT * FROM " + tableName + " WHERE 1 = 2");

            r = statement.getResultSet();

            ResultSetMetaData m    = r.getMetaData();
            int               cols = m.getColumnCount();

            for (int i = 0; i < cols; i++) {
                fieldArray    = new String[4];
                fieldArray[0] = m.getColumnName(i + 1);

                if (filter != null && (!filterMatchesAll)
                        && !filter.matcher(fieldArray[0]).find()) {
                    continue;
                }

                fieldArray[1] = m.getColumnTypeName(i + 1);
                fieldArray[2] = Integer.toString(m.getColumnDisplaySize(i + 1));
                fieldArray[3] =
                    ((m.isNullable(i + 1) == java.sql.ResultSetMetaData.columnNullable)
                     ? (htmlMode ? "&nbsp;"
                                 : "")
                     : "*");

                if (filter != null && filterMatchesAll
                        && !filter.matcher(fieldArray[0]
                            + ' ' + fieldArray[1] + ' ' + fieldArray[2] + ' '
                            + fieldArray[3]).find()) {
                    continue;
                }

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
                condlPrint(((i > 0) ? "  " : "")
                        + ((i < headerArray.length - 1 || rightJust[i])
                           ? StringUtil.toPaddedString(
                             headerArray[i], maxWidth[i], ' ', !rightJust[i])
                           : headerArray[i])
                        , false);
            }

            condlPrintln(LS + PRE_TR + "</TR>", true);
            condlPrintln("", false);

            if (!htmlMode) {
                for (int i = 0; i < headerArray.length; i++) {
                    condlPrint(((i > 0) ? "  "
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
                    condlPrint(((j > 0) ? "  " : "")
                            + ((j < fieldArray.length - 1 || rightJust[j])
                               ? StringUtil.toPaddedString(
                                 fieldArray[j], maxWidth[j], ' ', !rightJust[j])
                               : fieldArray[j])
                            , false);
                }

                condlPrintln(LS + PRE_TR + "</TR>", true);
                condlPrintln("", false);
            }

            condlPrintln(LS + "</TABLE>" + LS + "<HR>", true);
        } finally {
            try {
                if (r != null) {
                    r.close();
                }

                statement.close();
            } catch (SQLException se) {
                // Purposefully doing nothing
            }
        }
    }

    private boolean eval(String[] inTokens) throws BadSpecial {
        /* TODO:  Rewrite using java.util.regex.  */
        // dereference *VARNAME variables.
        // N.b. we work with a "copy" of the tokens.
        boolean  negate = inTokens.length > 0 && inTokens[0].equals("!");
        String[] tokens = new String[negate ? (inTokens.length - 1)
                                            : inTokens.length];
        String inToken;

        for (int i = 0; i < tokens.length; i++) {
            inToken = inTokens[i + (negate ? 1 : 0)];
            if (inToken.length() > 1 && inToken.charAt(0) == '*') {
                tokens[i] = (String) userVars.get(inToken.substring(1));
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

        throw new BadSpecial(rb.getString(SqltoolRB.LOGICAL_UNRECOGNIZED));
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

    private String formatNicely(Map map, boolean withValues) {
        String       key;
        StringBuffer sb = new StringBuffer();
        Iterator     it = (new TreeMap(map)).keySet().iterator();

        if (withValues) {
            SqlFile.appendLine(sb, rb.getString(SqltoolRB.PL_LIST_PARENS));
        } else {
            SqlFile.appendLine(sb, rb.getString(SqltoolRB.PL_LIST_LENGTHS));
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
     *
     * dumpFile must not be null.
     */
    private void dump(String varName,
                      File dumpFile) throws IOException, BadSpecial {
        String val = (String) userVars.get(varName);

        if (val == null) {
            throw new BadSpecial(rb.getString(
                    SqltoolRB.PLVAR_UNDEFINED, varName));
        }

        OutputStreamWriter osw = ((charset == null)
                ? (new OutputStreamWriter(new FileOutputStream(dumpFile)))
                : (new OutputStreamWriter(new FileOutputStream(dumpFile),
                            charset)));
        // Replace with just "(new FileOutputStream(file), charset)"
        // once use defaultCharset from Java 1.5 in charset init. above.

        try {
            osw.write(val);

            if (val.length() > 0) {
                char lastChar = val.charAt(val.length() - 1);

                if (lastChar != '\n' && lastChar != '\r') {
                    osw.write(LS);
                }
            }

            osw.flush();
        } finally {
            osw.close();
        }

        // Since opened in overwrite mode, since we didn't exception out,
        // we can be confident that we wrote all the bytest in the file.
        stdprintln(rb.getString(SqltoolRB.FILE_WROTECHARS,
                Long.toString(dumpFile.length()), dumpFile.toString()));
    }

    byte[] binBuffer = null;

    /**
     * Binary file dump
     *
     * dumpFile must not be null.
     */
    private void dump(File dumpFile) throws IOException, BadSpecial {
        if (binBuffer == null) {
            throw new BadSpecial(rb.getString(SqltoolRB.BINBUFFER_EMPTY));
        }

        FileOutputStream fos = new FileOutputStream(dumpFile);
        int len = 0;

        try {
            fos.write(binBuffer);

            len = binBuffer.length;

            binBuffer = null;

            fos.flush();
        } finally {
            fos.close();
        }
        stdprintln(rb.getString(SqltoolRB.FILE_WROTECHARS,
                len, dumpFile.toString()));
    }

    /**
     * As the name says...
     * This method always closes the input stream.
     */
    public String streamToString(InputStream is, String cs)
            throws IOException {
        try {
            byte[] ba = null;
            int bytesread = 0;
            int retval;
            try {
                ba = new byte[is.available()];
            } catch (RuntimeException re) {
                throw new IOException(rb.getString(SqltoolRB.READ_TOOBIG));
            }
            while (bytesread < ba.length &&
                    (retval = is.read(
                            ba, bytesread, ba.length - bytesread)) > 0) {
                bytesread += retval;
            }
            if (bytesread != ba.length) {
                throw new IOException(rb.getString(SqltoolRB.READ_PARTIAL,
                            bytesread,
                            ba.length));
            }
            try {
                return (cs == null) ? (new String(ba))
                                         : (new String(ba, cs));
            } catch (UnsupportedEncodingException uee) {
                throw new IOException(rb.getString(SqltoolRB.ENCODE_FAIL,
                        uee.getMessage()));
            } catch (RuntimeException re) {
                throw new IOException(rb.getString(SqltoolRB.READ_CONVERTFAIL));
            }
        } finally {
            is.close();
        }
    }

    /**
     * Ascii file load.
     */
    private void load(String varName, File asciiFile, String cs)
            throws IOException {
        String string = streamToString(new FileInputStream(asciiFile), cs);
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
        int                   i;
        FileInputStream       fis        = new FileInputStream(binFile);

        try {
            while ((i = fis.read(xferBuffer)) > 0) {
                baos.write(xferBuffer, 0, i);
            }
        } finally {
            fis.close();
        }

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

    // won't compile with JDK 1.4 without these
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

            case org.hsqldb.types.Types.SQL_TIME_WITH_TIME_ZONE :
                return "SQL_TIME_WITH_TIME_ZONE";

            case org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return "SQL_TIMESTAMP_WITH_TIME_ZONE";
        }

        return "Unknown type " + i;
    }

    /**
     * Validate that String is safe to write TO DSV file.
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
            throw new SqlToolError(rb.getString(SqltoolRB.DSV_COLDELIM_PRESENT,
                        dsvColDelim));
        }

        if (s.indexOf(dsvRowDelim) > 0) {
            throw new SqlToolError(rb.getString(SqltoolRB.DSV_ROWDELIM_PRESENT,
                        dsvRowDelim));
        }

        if (s.trim().equals(nullRepToken)) {
            // The trim() is to avoid the situation where the contents of a
            // field "looks like" the null-rep token.
            throw new SqlToolError(rb.getString(SqltoolRB.DSV_NULLREP_PRESENT,
                        nullRepToken));
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
            post = firstDigit + 1;
            while (post < string.length()
                    && Character.isDigit(string.charAt(post))) post++;
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
        /* To make string comparisons, contains() methods, etc. a little
         * simpler and concise, just switch all column names to lower-case.
         * This is ok since we acknowledge up front that DSV import/export
         * assume no special characters or escaping in column names. */
        Matcher matcher;
        byte[] bfr  = null;
        File   dsvFile = new File(filePath);
        SortedMap constColMap = null;
        if (dsvConstCols != null) {
            // We trim col. names, but not values.  Must allow users to
            // specify values as spaces, empty string, null.
            constColMap = new TreeMap();
            String[] constPairs = dsvConstCols.split(dsvColSplitter, -1);
            for (int i = 0; i < constPairs.length; i++) {
                matcher = nameValPairPattern.matcher(constPairs[i]);
                if (!matcher.matches()) {
                    throw new SqlToolError(
                            rb.getString(SqltoolRB.DSV_CONSTCOLS_NULLCOL));
                }
                constColMap.put(matcher.group(1).toLowerCase(),
                        ((matcher.groupCount() < 2 || matcher.group(2) == null)
                        ? "" : matcher.group(2)));
            }
        }
        Set skipCols = null;
        if (dsvSkipCols != null) {
            skipCols = new HashSet();
            String[] skipColsArray = dsvSkipCols.split(dsvColSplitter, -1);
            for (int i = 0; i < skipColsArray.length; i++) {
                skipCols.add(skipColsArray[i].trim().toLowerCase());
            }
        }

        if (!dsvFile.canRead()) {
            throw new SqlToolError(rb.getString(SqltoolRB.FILE_READFAIL,
                    dsvFile.toString()));
        }

        try {
            bfr = new byte[(int) dsvFile.length()];
        } catch (RuntimeException re) {
            throw new SqlToolError(rb.getString(SqltoolRB.READ_TOOBIG), re);
        }

        int bytesread = 0;
        int retval;
        InputStream is = null;

        try {
            is = new FileInputStream(dsvFile);
            while (bytesread < bfr.length &&
                    (retval = is.read(bfr, bytesread, bfr.length - bytesread))
                    > 0) {
                bytesread += retval;
            }

        } catch (IOException ioe) {
            throw new SqlToolError(ioe);
        } finally {
            if (is != null) try {
                is.close();
            } catch (IOException ioe) {
                errprintln(rb.getString(SqltoolRB.INPUTFILE_CLOSEFAIL)
                        + ": " + ioe);
            }
        }
        if (bytesread != bfr.length) {
            throw new SqlToolError(rb.getString(SqltoolRB.READ_PARTIAL,
                    bytesread, bfr.length));
        }

        String dateString;
        String[] lines = null;

        try {
            String string = ((charset == null)
                    ? (new String(bfr)) : (new String(bfr, charset)));
            lines = string.split(dsvRowSplitter, -1);
        } catch (UnsupportedEncodingException uee) {
            // Should not abort the program entirely, which this will do.
            throw new RuntimeException(uee);
        } catch (RuntimeException re) {
            throw new SqlToolError(rb.getString(SqltoolRB.READ_CONVERTFAIL),
                    re);
        }

        List     headerList = new ArrayList();
        String    tableName = dsvTargetTable;

        // First read one until we get one header line
        int lineCount = 0;
        String trimmedLine = null;
        boolean switching = false;
        int headerOffset = 0;  //  Used to offset read-start of header record
        String curLine = "dummy"; // Val will be replaced 4 lines down
                                  // This is just to quiet compiler warning

        while (true) {
            if (lineCount >= lines.length)
                throw new SqlToolError(rb.getString(SqltoolRB.DSV_HEADER_NONE));
            curLine = lines[lineCount++];
            trimmedLine = curLine.trim();
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
                    throw new SqlToolError(rb.getString(
                            SqltoolRB.DSV_HEADER_NOSWITCHTARG, lineCount));
                }
                switching = true;
                continue;
            }
            if (trimmedLine.equals("}")) {
                throw new SqlToolError(rb.getString(
                        SqltoolRB.DSV_HEADER_NOSWITCHMATCH, lineCount));
            }
            if (!switching) {
                break;
            }
            int colonAt = trimmedLine.indexOf(':');
            if (colonAt < 1 || colonAt == trimmedLine.length() - 1) {
                throw new SqlToolError(rb.getString(
                        SqltoolRB.DSV_HEADER_NONSWITCHED, lineCount));
            }
            String headerName = trimmedLine.substring(0, colonAt).trim();
            // Need to be sure here that tableName is not null (in
            // which case it would be determined later on by the file name).
            if (headerName.equals("*")
                    || headerName.equalsIgnoreCase(tableName)){
                headerOffset = 1 + curLine.indexOf(':');
                break;
            }
            // Skip non-matched header line
        }

        String headerLine = curLine.substring(headerOffset);
        String colName;
        String[] cols = headerLine.split(dsvColSplitter, -1);

        for (int i = 0; i < cols.length; i++) {
            if (cols[i].length() < 1) {
                throw new SqlToolError(rb.getString(
                        SqltoolRB.DSV_NOCOLHEADER,
                        headerList.size() + 1, lineCount));
            }

            colName = cols[i].trim().toLowerCase();
            headerList.add(
                (colName.equals("-")
                        || (skipCols != null
                                && skipCols.remove(colName))
                        || (constColMap != null
                                && constColMap.containsKey(colName))
                )
                ? ((String) null)
                : colName);
        }
        if (skipCols != null && skipCols.size() > 0) {
            throw new SqlToolError(rb.getString(
                    SqltoolRB.DSV_SKIPCOLS_MISSING, skipCols.toString()));
        }

        boolean oneCol = false;  // At least 1 non-null column
        for (int i = 0; i < headerList.size(); i++) {
            if (headerList.get(i) != null) {
                oneCol = true;
                break;
            }
        }
        if (oneCol == false) {
            // Difficult call, but I think in any real-world situation, the
            // user will want to know if they are inserting records with no
            // data from their input file.
            throw new SqlToolError(rb.getString(SqltoolRB.DSV_NOCOLSLEFT,
                    dsvSkipCols));
        }

        int inputColHeadCount = headerList.size();

        if (constColMap != null) {
            headerList.addAll(constColMap.keySet());
        }

        String[]  headers   = (String[]) headerList.toArray(new String[0]);
        // headers contains input headers + all constCols, some of these
        // values may be nulls.

        if (tableName == null) {
            tableName = dsvFile.getName();

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
        char[] readFormat = new char[autonulls.length];
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
                throw new SqlToolError(rb.getString(
                        SqltoolRB.DSV_METADATA_MISMATCH));
                // Don't know if it's possible to get here.
                // If so, it's probably a SqlTool problem, not a user or
                // data problem.
                // Should be researched and either return a user-friendly
                // message or a RuntimeExceptin.
            }

            for (int i = 0; i < autonulls.length; i++) {
                autonulls[i] = true;
                parseDate[i] = false;
                parseBool[i] = false;
                readFormat[i] = 's'; // regular Strings
                switch(rsmd.getColumnType(i + 1)) {
                    case java.sql.Types.BIT :
                        autonulls[i] = true;
                        readFormat[i] = 'b';
                        break;
                    case java.sql.Types.LONGVARBINARY :
                    case java.sql.Types.VARBINARY :
                    case java.sql.Types.BINARY :
                        autonulls[i] = true;
                        readFormat[i] = 'x';
                        break;
                    case java.sql.Types.BOOLEAN:
                        parseBool[i] = true;
                        break;
                    case java.sql.Types.VARCHAR :
                    case java.sql.Types.ARRAY :
                        // Guessing at how to handle ARRAY.
                    case java.sql.Types.BLOB :
                    case java.sql.Types.CLOB :
                    case java.sql.Types.LONGVARCHAR :
                        autonulls[i] = false;
                        // This means to preserve white space and to insert
                        // "" for "".  Otherwise we trim white space and
                        // insert null for \s*.
                        break;
                    case java.sql.Types.DATE:
                    case java.sql.Types.TIME:
                    case java.sql.Types.TIMESTAMP:
                    case org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE:
                    case org.hsqldb.types.Types.SQL_TIME_WITH_TIME_ZONE:
                        parseDate[i] = true;
                }
            }
        } catch (SQLException se) {
            throw new SqlToolError(rb.getString(
                    SqltoolRB.QUERY_METADATAFAIL,
                            typeQuerySb.toString()), se);
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
        if (dsvRejectFile != null) try {
            rejectFile = new File(dsvRejectFile);
            rejectWriter = new PrintWriter((charset == null)
                    ? (new OutputStreamWriter(new FileOutputStream(rejectFile)))
                    : (new OutputStreamWriter(new FileOutputStream(rejectFile),
                            charset)));
                    // Replace with just "(new FileOutputStream(file), charset)"
                    // once use defaultCharset from Java 1.5 in charset init.
                    // above.
            rejectWriter.print(headerLine + dsvRowDelim);
        } catch (IOException ioe) {
            throw new SqlToolError(rb.getString(
                    SqltoolRB.DSV_REJECTFILE_SETUPFAIL, dsvRejectFile), ioe);
        }
        if (dsvRejectReport != null) try {
            rejectReportFile = new File(dsvRejectReport);
            rejectReportWriter = new PrintWriter((charset == null)
                    ? (new OutputStreamWriter(
                            new FileOutputStream(rejectReportFile)))
                    : (new OutputStreamWriter(
                            new FileOutputStream(rejectReportFile), charset)));
                    // Replace with just "(new FileOutputStream(file), charset)"
                    // once use defaultCharset from Java 1.5 in charset init.
                    // above.
            rejectReportWriter.println(rb.getString(
                    SqltoolRB.REJECTREPORT_TOP, new String[] {
                        (new java.util.Date()).toString(),
                        dsvFile.getPath(),
                        ((rejectFile == null) ? rb.getString(SqltoolRB.NONE)
                                        : rejectFile.getPath()),
                        ((rejectFile == null) ? null : rejectFile.getPath()),
                    }));
        } catch (IOException ioe) {
            throw new SqlToolError(rb.getString(
                    SqltoolRB.DSV_REJECTREPORT_SETUPFAIL, dsvRejectReport),
                            ioe);
        }

        int recCount = 0;
        int skipCount = 0;
        PreparedStatement ps = null;
        boolean importAborted = false;
        boolean doResetAutocommit = false;
        try {
            doResetAutocommit = dsvRecordsPerCommit > 0
                && curConn.getAutoCommit();
            if (doResetAutocommit) curConn.setAutoCommit(false);
        } catch (SQLException se) {
            throw new SqlToolError(rb.getString(
                    SqltoolRB.RPC_AUTOCOMMIT_FAILURE), se);
        }
        // We're now assured that if dsvRecordsPerCommit is > 0, then
        // autocommit is off.

        try {
            try {
                ps = curConn.prepareStatement(sb.toString() + ')');
            } catch (SQLException se) {
                throw new SqlToolError(rb.getString(
                        SqltoolRB.INSERTION_PREPAREFAIL, sb.toString()), se);
            }
            String[] dataVals = new String[autonulls.length];
            // Length is number of cols to insert INTO, not nec. # in DSV file.
            int      readColCount;
            int      storeColCount;
            String   currentFieldName = null;

            // Insert data rows 1-row-at-a-time
            while (lineCount < lines.length) try { try {
                curLine = lines[lineCount++];
                trimmedLine = curLine.trim();
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
                        throw new SqlToolError(rb.getString(
                                SqltoolRB.DSV_HEADER_MATCHERNONHEAD,
                                        lineCount));
                    }
                    continue;
                }
                // Finished using "trimmed" line now.  Whitespace is
                // meaningful hereafter.

                // Finally we will attempt to add a record!
                recCount++;
                // Remember that recCount counts both inserts + rejects

                readColCount = 0;
                storeColCount = 0;
                cols = curLine.split(dsvColSplitter, -1);

                for (int coli = 0; coli < cols.length; coli++) {
                    if (readColCount == inputColHeadCount) {
                        throw new RowError(rb.getString(
                                SqltoolRB.DSV_COLCOUNT_MISMATCH,
                                        inputColHeadCount, 1 + readColCount));
                    }

                    if (headers[readColCount++] != null) {
                        dataVals[storeColCount++] =
                            dsvTrimAll ? cols[coli].trim() : cols[coli];
                    }
                }
                if (readColCount < inputColHeadCount) {
                    throw new RowError(rb.getString(
                            SqltoolRB.DSV_COLCOUNT_MISMATCH,
                                    inputColHeadCount, readColCount));
                }
                /* Already checked for readColCount too high in prev. block */

                if (constColMap != null) {
                    Iterator it = constColMap.values().iterator();
                    while (it.hasNext()) {
                        dataVals[storeColCount++] = (String) it.next();
                    }
                }
                if (storeColCount != dataVals.length) {
                    throw new RowError(rb.getString(
                            SqltoolRB.DSV_INSERTCOL_MISMATCH,
                                    dataVals.length, storeColCount));
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
                                throw new RowError(rb.getString(
                                        SqltoolRB.TIME_BAD, dateString), iae);
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
                                throw new RowError(rb.getString(
                                        SqltoolRB.BOOLEAN_BAD, dataVals[i]),
                                                iae);
                            }
                        }
                    } else {
                        switch (readFormat[i]) {
                            case 'b':
                                ps.setBytes(
                                    i + 1,
                                    (dataVals[i].length() < 1) ? null
                                    : SqlFile.bitCharsToBytes(
                                        dataVals[i]));
                                break;
                            case 'x':
                                ps.setBytes(
                                    i + 1,
                                    (dataVals[i].length() < 1) ? null
                                    : SqlFile.hexCharOctetsToBytes(
                                        dataVals[i]));
                                break;
                            default:
                                ps.setString(
                                    i + 1,
                                    (((dataVals[i].length() < 1 && autonulls[i])
                                      || dataVals[i].equals(nullRepToken))
                                     ? null
                                     : dataVals[i]));
                        }
                    }
                    currentFieldName = null;
                }

                retval = ps.executeUpdate();

                if (retval != 1) {
                    throw new RowError(rb.getString(
                            SqltoolRB.INPUTREC_MODIFIED, retval));
                }

                if (dsvRecordsPerCommit > 0
                    && (recCount - rejectCount) % dsvRecordsPerCommit == 0) {
                    curConn.commit();
                    possiblyUncommitteds.set(false);
                } else {
                    possiblyUncommitteds.set(true);
                }
            } catch (NumberFormatException nfe) {
                throw new RowError(null, nfe);
            } catch (SQLException se) {
                throw new RowError(null, se);
            } } catch (RowError re) {
                rejectCount++;
                if (rejectWriter != null || rejectReportWriter != null) {
                    if (rejectWriter != null) {
                        rejectWriter.print(curLine + dsvRowDelim);
                    }
                    if (rejectReportWriter != null) {
                        genRejectReportRecord(rejectReportWriter,
                                rejectCount, lineCount,
                                currentFieldName, re.getMessage(),
                                re.getCause());
                    }
                } else {
                    importAborted = true;
                    throw new SqlToolError(
                            rb.getString(SqltoolRB.DSV_RECIN_FAIL,
                                    lineCount, currentFieldName)
                            + ((re.getMessage() == null)
                                    ? "" : ("  " + re.getMessage())),
                            re.getCause());
                }
            }
        } finally {
            if (ps != null) try {
                ps.close();
            } catch (SQLException se) {
                // We already got what we want from it, or have/are
                // processing a more specific error.
            }
            try {
                if (dsvRecordsPerCommit > 0
                    && (recCount - rejectCount) % dsvRecordsPerCommit != 0) {
                    // To be consistent, if *DSV_RECORDS_PER_COMMIT is set, we
                    // always commit all inserted records.
                    // This little block commits any straggler commits since the
                    // last commit.
                    curConn.commit();
                    possiblyUncommitteds.set(false);
                }
                if (doResetAutocommit) curConn.setAutoCommit(true);
            } catch (SQLException se) {
                throw new SqlToolError(rb.getString(
                        SqltoolRB.RPC_COMMIT_FAILURE), se);
            }
            String summaryString = null;
            if (recCount > 0) {
                summaryString = rb.getString(SqltoolRB.DSV_IMPORT_SUMMARY,
                        new String[] {
                                ((skipPrefix == null)
                                          ? "" : ("'" + skipPrefix + "'-")),
                                Integer.toString(skipCount),
                                Integer.toString(rejectCount),
                                Integer.toString(recCount - rejectCount),
                                (importAborted ? "importAborted" : null)
                        });
                stdprintln(summaryString);
            }
            try {
                if (recCount > rejectCount && dsvRecordsPerCommit < 1
                        && !curConn.getAutoCommit()) {
                    stdprintln(rb.getString(SqltoolRB.INSERTIONS_NOTCOMMITTED));
                }
            } catch (SQLException se) {
                stdprintln(rb.getString(SqltoolRB.AUTOCOMMIT_FETCHFAIL));
                stdprintln(rb.getString(SqltoolRB.INSERTIONS_NOTCOMMITTED));
                // No reason to throw here.  If user attempts to use the
                // connection for anything significant, we will throw then.
            }
            if (rejectWriter != null) {
                rejectWriter.flush();
                rejectWriter.close();
            }
            if (rejectReportWriter != null && rejectCount > 0) {
                rejectReportWriter.println(rb.getString(
                        SqltoolRB.REJECTREPORT_BOTTOM, summaryString, revnum));
                rejectReportWriter.flush();
                rejectReportWriter.close();
            }
            if (rejectCount == 0) {
                if (rejectFile != null && rejectFile.exists()
                        && !rejectFile.delete())
                    errprintln(rb.getString(SqltoolRB.DSV_REJECTFILE_PURGEFAIL,
                            rejectFile.toString()));
                if (rejectReportFile != null && !rejectReportFile.delete())
                    errprintln(rb.getString(
                            SqltoolRB.DSV_REJECTREPORT_PURGEFAIL,
                                    (rejectFile == null)
                                            ? null : rejectFile.toString()));
                // These are trivial errors.
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
                internalTokens = m.group(i).split("\\s+", -1);
                for (int j = 0; j < internalTokens.length; j++)
                    list.add(internalTokens[j]);
            }
        }
        return (String[]) list.toArray(new String[] {});
    }

    private void genRejectReportRecord(PrintWriter pw, int rCount,
            int lCount, String field, String eMsg, Throwable cause) {
        pw.println(rb.getString(SqltoolRB.REJECTREPORT_ROW,
                new String[] {
                    ((rCount % 2 == 0) ? "even" : "odd") + "row",
                    Integer.toString(rCount),
                    Integer.toString(lCount),
                    ((field == null) ? "&nbsp;" : field),
                    (((eMsg == null) ? "" : eMsg)
                            + ((eMsg == null || cause == null) ? "" : "<HR/>")
                            + ((cause == null) ? "" : (
                                    (cause instanceof SQLException
                                            && cause.getMessage() != null)
                                        ? cause.getMessage()
                                        : cause.toString()
                                    )
                            )
                    )
                }));
    }

    /**
     * Parses input into command tokens, but does not perform the commands
     * (unless you consider parsing blocks of nested commands to be
     * "performing" a command).
     *
     * Throws only if I/O error or EOF encountered before end of entire file
     * (encountered at any level of recursion).
     *
     * Exceptions thrown within this method percolate right up to the
     * external call (in scanpass), regardless of ContinueOnErr setting.
     * This is because it's impossible to know when to terminate blocks
     * if there is a parsing error.
     * Only a separate SqlFile invocation (incl. \i command) will cause
     * a seekTokenSource exception to be handled at a level other than
     * the very top.
     */
    private TokenList seekTokenSource(String nestingCommand)
            throws BadSpecial, IOException {
        Token token;
        TokenList newTS = new TokenList();
        Pattern endPattern = Pattern.compile("end\\s+" + nestingCommand);
        String subNestingCommand;

        while ((token = scanner.yylex()) != null) {
            if (token.type == Token.PL_TYPE
                    && endPattern.matcher(token.val).matches()) {
                return newTS;
            }
            subNestingCommand = nestingCommand(token);
            if (subNestingCommand != null) {
                token.nestedBlock = seekTokenSource(subNestingCommand);
            }
            newTS.add(token);
        }
        throw new BadSpecial(rb.getString(
                SqltoolRB.PL_BLOCK_UNTERMINATED, nestingCommand));
    }

    /**
     * We want leading space to be trimmed.
     * Leading space should probably not be trimmed, but it is trimmed now
     * (by the Scanner).
     */
    private void processMacro(Token defToken) throws BadSpecial {
        Matcher matcher;
        Token macroToken;

        if (defToken.val.length() < 1) {
            throw new BadSpecial(rb.getString(SqltoolRB.MACRO_TIP));
        }
        switch (defToken.val.charAt(0)) {
            case '?':
                stdprintln(rb.getString(SqltoolRB.MACRO_HELP));
                break;
            case '=':
                String defString = defToken.val;
                defString = defString.substring(1).trim();
                if (defString.length() < 1) {
                    Iterator it = macros.keySet().iterator();
                    String key;
                    while (it.hasNext()) {
                        key = (String) it.next();
                        Token t = (Token) macros.get(key);
                        stdprintln(key + " = " + t.reconstitute());
                    }
                    break;
                }

                int newType = -1;
                StringBuffer newVal = new StringBuffer();
                matcher = editMacroPattern.matcher(defString);
                if (matcher.matches()) {
                    if (buffer == null) {
                        stdprintln(nobufferYetString);
                        return;
                    }
                    newVal.append(buffer.val);
                    if (matcher.groupCount() > 1 && matcher.group(2) != null
                            && matcher.group(2).length() > 0)
                        newVal.append(matcher.group(2));
                    newType = buffer.type;
                } else {
                    matcher = spMacroPattern.matcher(defString);
                    if (matcher.matches()) {
                        newVal.append(matcher.group(3));
                        newType = (matcher.group(2).equals("*")
                                ?  Token.PL_TYPE : Token.SPECIAL_TYPE);
                    } else {
                        matcher = sqlMacroPattern.matcher(defString);
                        if (!matcher.matches())
                            throw new BadSpecial(rb.getString(
                                    SqltoolRB.MACRO_MALFORMAT));
                        newVal.append(matcher.group(2));
                        newType = Token.SQL_TYPE;
                    }
                }
                if (newVal.length() < 1)
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.MACRODEF_EMPTY));
                if (newVal.charAt(newVal.length() - 1) == ';')
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.MACRODEF_SEMI));
                macros.put(matcher.group(1),
                        new Token(newType, newVal, defToken.line));
                break;
            default:
                matcher = useMacroPattern.matcher(defToken.val);
                if (!matcher.matches())
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.MACRO_MALFORMAT));
                macroToken = (Token) macros.get(matcher.group(1));
                if (macroToken == null)
                    throw new BadSpecial(rb.getString(
                            SqltoolRB.MACRO_UNDEFINED, matcher.group(1)));
                setBuf(macroToken);
                buffer.line = defToken.line;
                if (matcher.groupCount() > 1 && matcher.group(2) != null
                        && matcher.group(2).length() > 0)
                    buffer.val += matcher.group(2);
                preempt = matcher.group(matcher.groupCount()).equals(";");
        }
    }

    static public byte[] hexCharOctetsToBytes(String hexChars) {
        int chars = hexChars.length();
        if (chars != (chars / 2) * 2) {
            throw new NumberFormatException("Hex character lists contains "
                + "an odd number of characters: " + chars);
        }
        byte[] ba = new byte[chars/2];
        int offset = 0;
        char c;
        int octet;
        for (int i = 0; i < chars; i++) {
            octet = 0;
            c = hexChars.charAt(i);
            if (c >= 'a' && c <= 'f') {
                octet += 10 + c - 'a';
            } else if (c >= 'A' && c <= 'F') {
                octet += 10 + c - 'A';
            } else if (c >= '0' && c <= '9') {
                octet += c - '0';
            } else {
                throw new NumberFormatException(
                    "Non-hex character in input at offset " + i + ": " + c);
            }
            octet = octet << 4;
            c = hexChars.charAt(++i);
            if (c >= 'a' && c <= 'f') {
                octet += 10 + c - 'a';
            } else if (c >= 'A' && c <= 'F') {
                octet += 10 + c - 'A';
            } else if (c >= '0' && c <= '9') {
                octet += c - '0';
            } else {
                throw new NumberFormatException(
                    "Non-hex character in input at offset " + i + ": " + c);
            }

            ba[offset++] = (byte) octet;
        }
        if (ba.length != offset) {
            throw new RuntimeException(
                    "Internal accounting problem.  Expected to fill buffer of "
                    + "size "+ ba.length + ", but wrote only " + offset
                    + " bytes");
        }
        return ba;
    }

    static public byte[] bitCharsToBytes(String hexChars) {
        throw new NumberFormatException(
                "Sorry.  Bit exporting not supported yet");
    }
}
