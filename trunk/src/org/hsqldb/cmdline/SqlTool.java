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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.ValidatingResourceBundle;
import org.hsqldb.lib.RCData;

/* $Id$ */

/**
 * A command-line JDBC SQL tool supporting both interactive and
 * non-interactive usage.
 *
 * See JavaDocs for the main method for syntax of how to run from the
 * command-line.
 * <P/>
 * Programmatic users will usually want to use the objectMain(String[]) method
 * if they want arguments and behavior exactly like command-line SqlTool.
 * But in most cases, you would have better control and efficiency by using
 * the SqlFile class directly.  The file
 * <CODE>src/org/hsqldb/sample/SqlFileEmbedder.java</CODE>
 * in the HSQLDB distribution provides an example for this latter strategy.
 *
 * @see <a href="../../../../util-guide/sqltool-chapt.html" target="guide">
 *     The SqlTool chapter of the
 *     HyperSQL Utilities Guide</a>
 * @see #main(String[])
 * @see #objectMain(String[])
 * @see SqlFile
 * @see org.hsqldb.sample.SqlFileEmbedder
 * @version $Revision$, $Date$
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */
public class SqlTool {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(SqlTool.class);
    private static final String DEFAULT_RCFILE =
        System.getProperty("user.home") + "/sqltool.rc";
    // N.b. the following is static!
    private static String  revnum = null;

    public static final int SQLTOOLERR_EXITVAL = 1;
    public static final int SYNTAXERR_EXITVAL = 11;
    public static final int RCERR_EXITVAL = 2;
    public static final int SQLERR_EXITVAL = 3;
    public static final int IOERR_EXITVAL = 4;
    public static final int FILEERR_EXITVAL = 5;
    public static final int INPUTERR_EXITVAL = 6;
    public static final int CONNECTERR_EXITVAL = 7;

    /**
     * The configuration identifier to use when connection parameters are
     * specified on the command line
     */
    private static String CMDLINE_ID = "cmdline";
    private static SqltoolRB rb = null;
    // Must use a shared static RB object, since we need to get messages
    // inside of static methods.
    // This means that the locale will be set the first time this class
    // is accessed.  Subsequent calls will not update the RB if the locale
    // changes (could have it check and reload the RB if this becomes an
    // issue).

    static {
        revnum = "$Revision$".substring("$Revision: ".length(),
                                               "$Revision$".length()
                                               - 2);
        try {
            rb = new SqltoolRB();
            rb.validate();
            rb.setMissingPosValueBehavior(
                    ValidatingResourceBundle.NOOP_BEHAVIOR);
            rb.setMissingPropertyBehavior(
                    ValidatingResourceBundle.NOOP_BEHAVIOR);
        } catch (RuntimeException re) {
            System.err.println("Failed to initialize resource bundle");
            throw re;
        }
    }
    public static String LS = System.getProperty("line.separator");

    /** Utility nested class for internal use. */
    private static class BadCmdline extends Exception {
        static final long serialVersionUID = -2134764796788108325L;
        BadCmdline() {
            // Purposefully empty
        }
    }

    /** Utility object for internal use. */
    private static BadCmdline bcl = new BadCmdline();

    /** For trapping of exceptions inside this class.
     * These are always handled inside this class.
     */
    private static class PrivateException extends Exception {
        static final long serialVersionUID = -7765061479594523462L;

        PrivateException() {
            super();
        }

        PrivateException(String s) {
            super(s);
        }
    }

    public static class SqlToolException extends Exception {
        static final long serialVersionUID = 1424909871915188519L;

        int exitValue = 1;
        SqlToolException(String message, int exitValue) {
            super(message);
            this.exitValue = exitValue;
        }
        SqlToolException(int exitValue, String message) {
            this(message, exitValue);
        }
        SqlToolException(int exitValue) {
            super();
            this.exitValue = exitValue;
        }
    }

    /**
     * Prompt the user for a password.
     *
     * @param username The user the password is for
     * @return The password the user entered
     */
    private static String promptForPassword(String username)
    throws PrivateException {

        BufferedReader console;
        String         password;

        password = null;

        try {
            console = new BufferedReader(new InputStreamReader(System.in));

            // Prompt for password
            System.out.print(rb.getString(SqltoolRB.PASSWORDFOR_PROMPT,
                    RCData.expandSysPropVars(username)));

            // Read the password from the command line
            password = console.readLine();

            if (password == null) {
                password = "";
            } else {
                password = password.trim();
            }
        } catch (IOException e) {
            throw new PrivateException(e.getMessage());
        }

        return password;
    }

    /**
     * Parses a comma delimited string of name value pairs into a
     * <code>Map</code> object.
     *
     * @param varString The string to parse
     * @param varMap The map to save the paired values into
     * @param lowerCaseKeys Set to <code>true</code> if the map keys should be
     *        converted to lower case
     */
    private static void varParser(String varString, Map varMap,
                                  boolean lowerCaseKeys)
                                  throws PrivateException {

        int       equals;
        String    var;
        String    val;
        String[]  allvars;

        if ((varMap == null) || (varString == null)) {
            throw new IllegalArgumentException(
                    "varMap or varString are null in SqlTool.varParser call");
        }

        allvars = varString.split("\\s*,\\s*");

        for (int i = 0; i < allvars.length; i++) {
            equals     = allvars[i].indexOf('=');

            if (equals < 1) {
                throw new PrivateException(
                    rb.getString(SqltoolRB.SQLTOOL_VARSET_BADFORMAT));
            }

            var = allvars[i].substring(0, equals).trim();
            val = allvars[i].substring(equals + 1).trim();

            if (var.length() < 1) {
                throw new PrivateException(
                    rb.getString(SqltoolRB.SQLTOOL_VARSET_BADFORMAT));
            }

            if (lowerCaseKeys) {
                var = var.toLowerCase();
            }

            varMap.put(var, val);
        }
    }

    /**
     * A static wrapper for objectMain, so that that method may be executed
     * as a Java "program".
     * <P>
     * Throws only RuntimeExceptions or Errors, because this method is intended
     * to System.exit() for all but disasterous system problems, for which
     * the inconvenience of a stack trace would be the least of your worries.
     * <P/> <P>
     * If you don't want SqlTool to System.exit(), then use the method
     * objectMain() instead of this method.
     * <P/>
     *
     * @see #objectMain(String[])
     */
    public static void main(String[] args) {
        try {
            SqlTool.objectMain(args);
        } catch (SqlToolException fr) {
            if (fr.getMessage() != null) {
                System.err.println(fr.getMessage());
            }
            System.exit(fr.exitValue);
        }
        System.exit(0);
    }

    /**
     * Connect to a JDBC Database and execute the commands given on
     * stdin or in SQL file(s).
     * <P/>
     * This method is changed for HSQLDB 1.8.0.8 and later to never
     * System.exit().
     *
     * @param arg  Run "java... org.hsqldb.cmdline.SqlTool --help" for syntax.
     * @throws SqlToolException  Upon any fatal error, with useful
     *                          reason as the exception's message.
     */
    static public void objectMain(String[] arg) throws SqlToolException {
        logger.finer("Invoking SqlTool");

        /*
         * The big picture is, we parse input args; load a RCData;
         * get a JDBC Connection with the RCData; instantiate and
         * execute as many SqlFiles as we need to.
         */
        String  rcFile           = null;
        File    tmpFile          = null;
        String  sqlText          = null;
        String  driver           = null;
        String  targetDb         = null;
        String  varSettings      = null;
        boolean debug            = false;
        File[]  scriptFiles      = null;
        int     i                = -1;
        boolean listMode         = false;
        boolean interactive      = false;
        boolean noinput          = false;
        boolean noautoFile       = false;
        boolean autoCommit       = false;
        Boolean coeOverride      = null;
        Boolean stdinputOverride = null;
        String  rcParams         = null;
        String  rcUrl            = null;
        String  rcUsername       = null;
        String  rcPassword       = null;
        String  rcCharset        = null;
        String  rcTruststore     = null;
        String  rcTransIso     = null;
        Map     rcFields         = null;
        String  parameter;

        try {
            while ((i + 1 < arg.length) && arg[i + 1].startsWith("--")) {
                i++;

                if (arg[i].length() == 2) {
                    break;             // "--"
                }

                parameter = arg[i].substring(2).toLowerCase();

                if (parameter.equals("help")) {
                    System.out.println(rb.getString(SqltoolRB.SQLTOOL_SYNTAX,
                            revnum, RCData.DEFAULT_JDBC_DRIVER));
                    return;
                }
                if (parameter.equals("abortonerr")) {
                    if (coeOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                rb.getString(
                        SqltoolRB.SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE));
                    }

                    coeOverride = Boolean.FALSE;
                } else if (parameter.equals("continueonerr")) {
                    if (coeOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                rb.getString(
                        SqltoolRB.SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE));
                    }

                    coeOverride = Boolean.TRUE;
                } else if (parameter.startsWith("continueonerr=")) {
                    if (coeOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                rb.getString(
                        SqltoolRB.SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE));
                    }

                    coeOverride = Boolean.valueOf(
                            arg[i].substring("--continueonerr=".length()));
                } else if (parameter.equals("list")) {
                    listMode = true;
                } else if (parameter.equals("rcfile")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }

                    rcFile = arg[i];
                } else if (parameter.startsWith("rcfile=")) {
                    rcFile = arg[i].substring("--rcfile=".length());
                } else if (parameter.equals("setvar")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }

                    varSettings = arg[i];
                } else if (parameter.startsWith("setvar=")) {
                    varSettings = arg[i].substring("--setvar=".length());
                } else if (parameter.equals("sql")) {
                    noinput = true;    // but turn back on if file "-" specd.

                    if (++i == arg.length) {
                        throw bcl;
                    }

                    sqlText = arg[i];
                } else if (parameter.startsWith("sql=")) {
                    noinput = true;    // but turn back on if file "-" specd.
                    sqlText = arg[i].substring("--sql=".length());
                } else if (parameter.equals("debug")) {
                    debug = true;
                } else if (parameter.equals("noautofile")) {
                    noautoFile = true;
                } else if (parameter.equals("autocommit")) {
                    autoCommit = true;
                } else if (parameter.equals("stdinput")) {
                    noinput          = false;
                    stdinputOverride = Boolean.TRUE;
                } else if (parameter.equals("noinput")) {
                    noinput          = true;
                    stdinputOverride = Boolean.FALSE;
                } else if (parameter.equals("driver")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }

                    driver = arg[i];
                } else if (parameter.startsWith("driver=")) {
                    driver = arg[i].substring("--driver=".length());
                } else if (parameter.equals("inlinerc")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }

                    rcParams = arg[i];
                } else if (parameter.startsWith("inlinerc=")) {
                    rcParams = arg[i].substring("--inlinerc=".length());
                } else {
                    throw bcl;
                }
            }

            if (!listMode) {

                // If an inline RC file was specified, don't worry about the targetDb
                if (rcParams == null) {
                    if (++i == arg.length) {
                        throw bcl;
                    }

                    targetDb = arg[i];
                }
            }

            int scriptIndex = 0;

            if (sqlText != null) {
                try {
                    tmpFile = File.createTempFile("sqltool-", ".sql");

                    //(new java.io.FileWriter(tmpFile)).write(sqlText);
                    java.io.FileWriter fw = new java.io.FileWriter(tmpFile);
                    try {

                        fw.write("/* " + (new java.util.Date()) + ".  "
                                 + SqlTool.class.getName()
                                 + " command-line SQL. */" + LS + LS);
                        fw.write(sqlText + LS);
                        fw.flush();
                    } finally {
                        fw.close();
                    }
                } catch (IOException ioe) {
                    throw new SqlToolException(IOERR_EXITVAL,
                            rb.getString(SqltoolRB.SQLTEMPFILE_FAIL,
                                    ioe.toString()));
                }
            }

            if (stdinputOverride != null) {
                noinput = !stdinputOverride.booleanValue();
            }

            interactive = (!noinput) && (arg.length <= i + 1);

            if (arg.length == i + 2 && arg[i + 1].equals("-")) {
                if (stdinputOverride == null) {
                    noinput = false;
                }
            } else if (arg.length > i + 1) {

                // I.e., if there are any SQL files specified.
                scriptFiles = new File[arg.length - i - 1
                        + ((stdinputOverride == null
                                ||!stdinputOverride.booleanValue()) ? 0 : 1)];

                if (debug) {
                    System.err.println("scriptFiles has "
                                       + scriptFiles.length + " elements");
                }

                while (i + 1 < arg.length) {
                    scriptFiles[scriptIndex++] = new File(arg[++i]);
                }

                if (stdinputOverride != null
                        && stdinputOverride.booleanValue()) {
                    scriptFiles[scriptIndex++] = null;
                    noinput                    = true;
                }
            }
        } catch (BadCmdline bcle) {
            throw new SqlToolException(SYNTAXERR_EXITVAL,
                    rb.getString(SqltoolRB.SQLTOOL_SYNTAX,
                                revnum, RCData.DEFAULT_JDBC_DRIVER));
        }

        RCData conData = null;

        // Use the inline RC file if it was specified
        if (rcParams != null) {
            rcFields = new HashMap();

            try {
                varParser(rcParams, rcFields, true);
            } catch (PrivateException e) {
                throw new SqlToolException(SYNTAXERR_EXITVAL, e.getMessage());
            }

            rcUrl        = (String) rcFields.remove("url");
            rcUsername   = (String) rcFields.remove("user");
            rcCharset    = (String) rcFields.remove("charset");
            rcTruststore = (String) rcFields.remove("truststore");
            rcPassword   = (String) rcFields.remove("password");
            rcTransIso   = (String) rcFields.remove("transiso");

            // Don't ask for password if what we have already is invalid!
            if (rcUrl == null || rcUrl.length() < 1)
                throw new SqlToolException(RCERR_EXITVAL, rb.getString(
                        SqltoolRB.RCDATA_INLINEURL_MISSING));
            // We now allow both null and "" user name, but we require password
            // if the user name != null.
            if (rcPassword != null && rcPassword.length() > 0)
                throw new SqlToolException(RCERR_EXITVAL, rb.getString(
                        SqltoolRB.RCDATA_PASSWORD_VISIBLE));
            if (rcFields.size() > 0) {
                throw new SqlToolException(INPUTERR_EXITVAL,
                        rb.getString(SqltoolRB.RCDATA_INLINE_EXTRAVARS,
                                rcFields.keySet().toString()));
            }

            if (rcUsername != null && rcPassword == null) try {
                rcPassword   = promptForPassword(rcUsername);
            } catch (PrivateException e) {
                throw new SqlToolException(INPUTERR_EXITVAL,
                        rb.getString(SqltoolRB.PASSWORD_READFAIL,
                                e.getMessage()));
            }
            try {
                conData = new RCData(CMDLINE_ID, rcUrl, rcUsername,
                                     rcPassword, driver, rcCharset,
                                     rcTruststore, null, rcTransIso);
            } catch (RuntimeException re) {
                throw re;  // Unrecoverable
            } catch (Exception e) {
                throw new SqlToolException(RCERR_EXITVAL, rb.getString(
                        SqltoolRB.RCDATA_GENFROMVALUES_FAIL, e.getMessage()));
            }
        } else {
            try {
                conData = new RCData(new File((rcFile == null)
                                              ? DEFAULT_RCFILE
                                              : rcFile), targetDb);
            } catch (RuntimeException re) {
                throw re;  // Unrecoverable
            } catch (Exception e) {
                throw new SqlToolException(RCERR_EXITVAL, rb.getString(
                        SqltoolRB.CONNDATA_RETRIEVAL_FAIL,
                                targetDb, e.getMessage()));
            }
        }

        if (listMode) {
            return;
        }

        //if (debug) {
            //conData.report();
        //}

        Connection conn = null;
        try {
            conn = conData.getConnection(
                driver, System.getProperty("javax.net.ssl.trustStore"));

            conn.setAutoCommit(autoCommit);

            DatabaseMetaData md = null;

            if (interactive && (md = conn.getMetaData()) != null) {
                System.out.println(
                        rb.getString(SqltoolRB.JDBC_ESTABLISHED,
                                md.getDatabaseProductName(),
                                md.getDatabaseProductVersion(),
                                md.getUserName(),
                                        (conn.isReadOnly() ? "R/O " : "R/W ")
                                        + RCData.tiToString(
                                        conn.getTransactionIsolation())));
            }
        } catch (RuntimeException re) {
            throw re;  // Unrecoverable
        } catch (Exception e) {
            if (debug) logger.error(e.getClass().getName(), e);

            // Let's not continue as if nothing is wrong.
            String reportUser = (conData.username == null)
                    ? "<DFLTUSER>" : conData.username;
            throw new SqlToolException(CONNECTERR_EXITVAL,
                    rb.getString(SqltoolRB.CONNECTION_FAIL, conData.url,
                    reportUser, e.getMessage()));
        }

        File[] emptyFileArray      = {};
        File[] singleNullFileArray = { null };
        File   autoFile            = null;

        if (interactive &&!noautoFile) {
            autoFile = new File(System.getProperty("user.home")
                                + "/auto.sql");

            if ((!autoFile.isFile()) ||!autoFile.canRead()) {
                autoFile = null;
            }
        }

        if (scriptFiles == null) {

            // I.e., if no SQL files given on command-line.
            // Input file list is either nothing or {null} to read stdin.
            scriptFiles = (noinput ? emptyFileArray
                                   : singleNullFileArray);
        }

        int numFiles = scriptFiles.length;

        if (tmpFile != null) {
            numFiles += 1;
        }

        if (autoFile != null) {
            numFiles += 1;
        }

        SqlFile[] sqlFiles = new SqlFile[numFiles];

        Map userVars = null;
        if (varSettings != null) try {
            varParser(varSettings, userVars, false);
        } catch (PrivateException pe) {
            throw new SqlToolException(RCERR_EXITVAL, pe.getMessage());
        }

        // We print version before execing this one.
        int interactiveFileIndex = -1;

        try {
            int fileIndex = 0;

            if (autoFile != null) {
                sqlFiles[fileIndex++] = new SqlFile(autoFile, conData.charset);
            }

            if (tmpFile != null) {
                sqlFiles[fileIndex++] = new SqlFile(tmpFile, conData.charset);
            }

            for (int j = 0; j < scriptFiles.length; j++) {
                if (interactiveFileIndex < 0 && interactive) {
                    interactiveFileIndex = fileIndex;
                }

                sqlFiles[fileIndex++] = (scriptFiles[j] == null)
                        ?  (new SqlFile(conData.charset, interactive))
                        :  (new SqlFile(scriptFiles[j],
                                conData.charset, interactive));
            }
        } catch (IOException ioe) {
            try {
                conn.close();
            } catch (Exception e) {
                // Can only report on so many errors at one time
            }

            throw new SqlToolException(FILEERR_EXITVAL, ioe.getMessage());
        }

        Map macros = null;
        try {
            for (int j = 0; j < sqlFiles.length; j++) {
                sqlFiles[j].setConnection(conn);
                if (userVars != null) sqlFiles[j].addUserVars(userVars);
                if (macros != null) sqlFiles[j].addUserVars(macros);
                if (coeOverride != null)
                    sqlFiles[j].setContinueOnError(coeOverride.booleanValue());
                if (j == interactiveFileIndex) {
                    System.out.print("SqlTool v. " + revnum
                                     + ".                        ");
                }

                sqlFiles[j].execute();
                userVars = sqlFiles[j].getUserVars();
                macros = sqlFiles[j].getMacros();
            }
            // Following two Exception types are handled properly inside of
            // SqlFile.  We just need to return an appropriate error status.
        } catch (SqlToolError ste) {
            throw new SqlToolException(SQLTOOLERR_EXITVAL);
        } catch (SQLException se) {
            // SqlTool will only throw an SQLException if it is in
            // "\c false" mode.
            throw new SqlToolException(SQLERR_EXITVAL);
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                // Purposefully doing nothing
            }
            if ((!debug) && tmpFile != null && !tmpFile.delete()) {
                // Leave this in final block.
                // There are plenty of valid use-cases where SqlTool is
                // expected to fail out, and we usually do not want files
                // left around.
                System.err.println(conData.url + rb.getString(
                        SqltoolRB.TEMPFILE_REMOVAL_FAIL, tmpFile.toString()));
            }
        }
    }
}
