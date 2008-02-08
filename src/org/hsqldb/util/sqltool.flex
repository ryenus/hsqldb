package org.hsqldb.util.sqltool;

import java.io.PrintStream;
import org.hsqldb.util.SqlFile.ToolLogger;

%%
// Defaults to Yylex
%class SqlFileScanner
%implements TokenSource
%{
    static private ToolLogger logger = ToolLogger.getLog(SqlFileScanner.class);
    private StringBuffer commandBuffer = new StringBuffer();
    private boolean interactive = false;
    private PrintStream psStd = System.out;
    private String magicPrefix = null;
    private int requestedState = YYINITIAL;

    public void setRequestedState(int requestedState) {
        this.requestedState = requestedState;
    }

    // Trims only the end
    private void trimBuffer() {
        int len = commandBuffer.length();
        commandBuffer.setLength(len -
            ((len > 1 && commandBuffer.charAt(len - 2) == '\r') ? 2 : 1));
    }

    public void setCommandBuffer(String s) {
        commandBuffer.setLength(0);
        commandBuffer.append(s);
    }

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    public void setMagicPrefix(String magicPrefix) {
        this.magicPrefix = magicPrefix;
    }
    
    public void setStdPrintStream(PrintStream psStd) {
        this.psStd = psStd;
    }

    //private String sqlPrompt = "+sql> ";
    private String sqlPrompt = null;
    public void setSqlPrompt(String sqlPrompt)
    {
        this.sqlPrompt = sqlPrompt;
    }
    public String getSqlPrompt() {
        return sqlPrompt;
    }

    //private String sqltoolPrompt = "sql> ";
    private String sqltoolPrompt = null;
    public void setSqltoolPrompt(String sqltoolPrompt)
    {
        this.sqltoolPrompt = sqltoolPrompt;
    }
    public String getSqltoolPrompt() {
        return sqltoolPrompt;
    }
    //private String rawPrompt = "raw> ";
    private String rawPrompt = null;
    public void setRawPrompt(String rawPrompt)
    {
        this.rawPrompt = rawPrompt;
    }
    public String getRawPrompt() {
        return rawPrompt;
    }

    private void debug(String id, String msg) {
        logger.finest(id + ":  [" + msg + ']');
    }

    public String strippedYytext() {
        String lineString = yytext();
        int len = lineString.length();
        len = len - ((len > 1 && lineString.charAt(len - 2) == '\r') ? 2 : 1);
        return (lineString.substring(0, len));
    }

    // Trims only the end
    public void pushbackTrim() {
        String lineString = yytext();
        int len = lineString.length();
        yypushback((len > 1 && lineString.charAt(len - 2) == '\r') ? 2 : 1);
    }

    private void prompt(String s) {
        if (!interactive) return;
        psStd.print(s);
    }

    public void prompt() {
        if (sqltoolPrompt != null) prompt(sqltoolPrompt);
        if (interactive && magicPrefix != null) {
            psStd.print(magicPrefix);
            magicPrefix = null;
        }
    }
%}

%public
//%int
%line
%column
%eofclose
%unicode
%type Token
%xstates SQL RAW SQL_SINGLE_QUOTED SQL_DOUBLE_QUOTED GOBBLE SPECIAL PL EDIT
%xstates MACRO PROMPT_CHANGE_STATE
/* Single-quotes Escaped with '',
 * In Oracle, at least, no inner double-quotes (i.e. no escaping)
 * SQL-Embedded comments are passed to SQL engine as part of SQL command */

/* Expressions could be simplified by using "." instead of "[^\r\n]", but
 * the docs say that "." means "[^n]", therefore this would mess up DOS-style
 * line endings. */

LINETERM_MAC = \r|\n|\r\n
SQL_STARTER = [^\n\r\t\f \\*:\"\']
TRADITIONAL_COMMENT = "/*" ~"*/"
/* CURLY_COMMENT = "{" ~"}"   Purposefully not supporting */

%%
<PROMPT_CHANGE_STATE> {LINETERM_MAC} {
    yybegin(requestedState);
    prompt();
}
<GOBBLE> ~{LINETERM_MAC} {
    yybegin(YYINITIAL);
    debug("Gobbled", yytext());
    prompt();
}
<SQL, SQL_SINGLE_QUOTED, SQL_DOUBLE_QUOTED, SPECIAL, PL, EDIT, MACRO> <<EOF>> {
    yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
}
{TRADITIONAL_COMMENT} { /* Ignore top-level traditional comments */
    debug ("/**/ Comment", yytext());
}
[ \f\t]+ { /* Ignore top-level whte space */
    debug("Whitespace", yytext());
}
{LINETERM_MAC} {
    prompt();
}
/*
 * TODO:  Comments ignored in SqlTool commands?  Check this!
 *        To implement, would need a sqlToolBuffer and IN_SQLTOOL xstate.
 */
[--][^\n\r]* {
    debug ("-- Comment", yytext());
}
; {
    return new Token(Token.SQL_TYPE, yyline);
}
[Bb][Ee][Gg][Ii][Nn] [\f\t ]* {LINETERM_MAC} |
[Dd][Ee][Cc][Ll][Aa][Rr][Ee] [\f\t ]* {LINETERM_MAC} {
    setCommandBuffer(strippedYytext());
    yybegin(RAW);
    if (rawPrompt != null) prompt(rawPrompt);
}
\* {
    commandBuffer.setLength(0);
    yybegin(PL);
}
\\ {
    commandBuffer.setLength(0);
    yybegin(SPECIAL);
}
\: {
    commandBuffer.setLength(0);
    yybegin(EDIT);
}
\/ {
    commandBuffer.setLength(0);
    yybegin(MACRO);
}
<RAW> {
    [\f\t ]*\.[\f\t ]* ; [\f\t ]* {LINETERM_MAC} {
        yybegin(YYINITIAL);
        prompt();
        return new Token(Token.RAWEXEC_TYPE, commandBuffer, yyline);
    }
    [\f\t ]*\.[\f\t ]* {LINETERM_MAC} {
        yybegin(YYINITIAL);
        prompt();
        return new Token(Token.RAW_TYPE, commandBuffer, yyline);
    }
    ~{LINETERM_MAC} {
        if (commandBuffer.length() > 0) commandBuffer.append('\n');
        commandBuffer.append(strippedYytext());
        if (rawPrompt != null) prompt(rawPrompt);
    }
}
<SPECIAL> {LINETERM_MAC} {
    if (commandBuffer.toString().trim().equals(".")) {
        commandBuffer.setLength(0);
        yybegin(RAW);
        if (rawPrompt != null) prompt(rawPrompt);
    } else {
        requestedState = YYINITIAL;
        yybegin(PROMPT_CHANGE_STATE);
        pushbackTrim();
        return new Token(Token.SPECIAL_TYPE, commandBuffer, yyline);
    }
}
<PL> {LINETERM_MAC} {
    requestedState = YYINITIAL;
    yybegin(PROMPT_CHANGE_STATE);
    pushbackTrim();
    return new Token(Token.PL_TYPE, commandBuffer, yyline);
}
<MACRO> {LINETERM_MAC} {
    requestedState = YYINITIAL;
    yybegin(PROMPT_CHANGE_STATE);
    pushbackTrim();
    return new Token(Token.MACRO_TYPE, commandBuffer, yyline);
}
<EDIT> {LINETERM_MAC} {
    requestedState = YYINITIAL;
    yybegin(PROMPT_CHANGE_STATE);
    pushbackTrim();
    return new Token(Token.EDIT_TYPE, commandBuffer, yyline);
}
<SPECIAL, PL, MACRO> {
    // Purposefully not allowing comments within :Edit commands
    {TRADITIONAL_COMMENT} {
        /* embedded comment may disable opening closing \n */
        debug("Spl. /**/ Comment", yytext());
    }
    "--" ~{LINETERM_MAC}  {
        pushbackTrim();
        /* embedded comment may disable opening quotes and closing ; */
        debug("Spl. -- Comment", yytext());
    }
}
<SPECIAL, EDIT, PL, MACRO> {
    [^\n\r] {
        commandBuffer.append(yytext());
    }
}
{SQL_STARTER} {
    setCommandBuffer(yytext());
    yybegin(SQL);
}
<SQL> {
    ^[\f\t ]* {LINETERM_MAC} {
        if (interactive) {
            requestedState = YYINITIAL;
            yybegin(PROMPT_CHANGE_STATE);
            pushbackTrim();
            trimBuffer();
            return new Token(Token.BUFFER_TYPE, commandBuffer, yyline);
        } else {
            commandBuffer.append(yytext());
        }
    }
    {TRADITIONAL_COMMENT} {
        commandBuffer.append(yytext());
        /* embedded comment may disable opening quotes and closing ; */
        debug("SQL /**/ Comment", yytext());
    }
    "--" ~{LINETERM_MAC} {
        commandBuffer.append(yytext());
        /* embedded comment may disable opening quotes and closing ; */
        debug("SQL -- Comment", yytext());
    }
    {LINETERM_MAC} {
        commandBuffer.append(yytext());
        if (sqlPrompt != null) prompt(sqlPrompt);
    }
    [^\"\';] {
        commandBuffer.append(yytext());
    }
    \' {
        /* TODO:  Find out if can escape " in SQL commands and handle! */
        commandBuffer.append(yytext());
        yybegin(SQL_SINGLE_QUOTED);
    }
    \" {
        /* TODO:  Find out if can escape " in SQL commands and handle! */
        commandBuffer.append(yytext());
        yybegin(SQL_DOUBLE_QUOTED);
    }
    ; {
        yybegin(YYINITIAL);
        return new Token(Token.SQL_TYPE, commandBuffer, yyline);
    }
}
<SQL_SINGLE_QUOTED> {
    [^\']+ {
        commandBuffer.append(yytext());
    }
    \'\' {
        commandBuffer.append(yytext());
    }
    \' {
        commandBuffer.append(yytext());
        debug("SQL '", yytext());
        yybegin(SQL);
    }
}
<SQL_DOUBLE_QUOTED> {
    [^\"]+ {
        commandBuffer.append(yytext());
    }
    \" {
        commandBuffer.append(yytext());
        yybegin(SQL);
        debug("SQL \"", yytext());
    }
}
[^\r\n] {
    yybegin(GOBBLE);
    return new Token(Token.SYNTAX_ERR_TYPE, yytext(), yyline);
}
