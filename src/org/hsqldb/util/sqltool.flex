package org.hsqldb.util.sqltool;

import java.io.PrintStream;

%%
// Defaults to Yylex
%class SqlFileScanner
%implements TokenSource
%{
    private StringBuffer sqlCommand = new StringBuffer();
    private StringBuffer specialCommand = new StringBuffer();
    private boolean interactive = false;
    private PrintStream psStd = System.out;
    private PrintStream psErr = System.err;

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }
    
    public void setStdPrintStream(PrintStream psStd) {
        this.psStd = psStd;
    }
    public void setErrPrintStream(PrintStream psErr) {
        this.psErr = psErr;
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
        psErr.println(id + ":  [" + msg + ']');
    }

    public String strippedYytext() {
        String lineString = yytext();
        int len = lineString.length() - ((lineString.length() > 0
                && lineString.charAt(lineString.length()-2) == '\r')
                ? 2 : 1);
        return (lineString.substring(0, len));
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
%xstates SQLTOOL_PROMPT
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
<SQLTOOL_PROMPT> [\r\n] {
    yybegin(YYINITIAL);
    if (sqlPrompt != null) psStd.print(sqltoolPrompt);
}
<GOBBLE> ~{LINETERM_MAC} {
    yybegin(YYINITIAL);
    debug("Gobbled", yytext());
    if (sqltoolPrompt != null) psStd.print(sqltoolPrompt);
}
<SQL, SQL_SINGLE_QUOTED, SQL_DOUBLE_QUOTED, SPECIAL, PL, EDIT> <<EOF>> {
    return new Token(Token.UNTERM_TYPE, sqlCommand, yyline);
}
{TRADITIONAL_COMMENT} { /* Ignore top-level traditional comments */
    debug ("/**/ Comment", yytext());
}
[ \f\t]+ { /* Ignore top-level whte space */
    debug("Whitespace", yytext());
}
{LINETERM_MAC} {
    if (sqltoolPrompt != null) psStd.print(sqltoolPrompt);
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
    sqlCommand.setLength(0);
    sqlCommand.append(strippedYytext());
    yybegin(RAW);
    if (rawPrompt != null) psStd.print(rawPrompt);
}
\* {
    specialCommand.setLength(0);
    yybegin(PL);
}
\\ {
    specialCommand.setLength(0);
    yybegin(SPECIAL);
}
\: {
    specialCommand.setLength(0);
    yybegin(EDIT);
}
<RAW> {
    [\f\t ]*\.[\f\t ]* ; [\f\t ]* {LINETERM_MAC} {
        yybegin(YYINITIAL);
        if (sqlPrompt != null) psStd.print(sqltoolPrompt);
        return new Token(Token.RAWEXEC_TYPE, sqlCommand, yyline);
    }
    [\f\t ]*\.[\f\t ]* {LINETERM_MAC} {
        yybegin(YYINITIAL);
        if (sqlPrompt != null) psStd.print(sqltoolPrompt);
        return new Token(Token.RAW_TYPE, sqlCommand, yyline);
    }
    ~{LINETERM_MAC} {
        if (sqlCommand.length() > 0) sqlCommand.append('\n');
        sqlCommand.append(strippedYytext());
        if (rawPrompt != null) psStd.print(rawPrompt);
    }
}
<SPECIAL> {LINETERM_MAC} {
    if (specialCommand.toString().trim().equals(".")) {
        sqlCommand.setLength(0);
        yybegin(RAW);
        if (rawPrompt != null) psStd.print(rawPrompt);
    } else {
        yybegin(SQLTOOL_PROMPT);
        yypushback(1);
        return new Token(Token.SPECIAL_TYPE, specialCommand, yyline);
    }
}
<PL> {LINETERM_MAC} {
    yybegin(SQLTOOL_PROMPT);
    yypushback(1);
    return new Token(Token.PL_TYPE, specialCommand, yyline);
}
<EDIT> {LINETERM_MAC} {
    yybegin(SQLTOOL_PROMPT);
    yypushback(1);
    return new Token(Token.EDIT_TYPE, specialCommand, yyline);
}
<SPECIAL, PL> {
    // Purposefully not allowing comments within :Edit commands
    {TRADITIONAL_COMMENT} {
        /* embedded comment may disable opening closing \n */
        debug("Spl. /**/ Comment", yytext());
    }
    "--" ~{LINETERM_MAC}  {
        yypushback(1);
        /* embedded comment may disable opening quotes and closing ; */
        debug("Spl. -- Comment", yytext());
    }
}
<SPECIAL, EDIT, PL> {
    [^\n\r] {
        specialCommand.append(yytext());
    }
}
{SQL_STARTER} {
    sqlCommand.setLength(0);
    sqlCommand.append(yytext());
    yybegin(SQL);
}
<SQL> {
    ^[\f\t ]* {LINETERM_MAC} {
        if (interactive) {
            yybegin(YYINITIAL);
            return new Token(Token.BUFFER_TYPE, sqlCommand, yyline);
        } else {
            sqlCommand.append(yytext());
            if (sqlPrompt != null) psStd.print(sqlPrompt);
        }
    }
    {TRADITIONAL_COMMENT} {
        sqlCommand.append(yytext());
        /* embedded comment may disable opening quotes and closing ; */
        debug("SQL /**/ Comment", yytext());
    }
    "--" ~{LINETERM_MAC} {
        sqlCommand.append(yytext());
        /* embedded comment may disable opening quotes and closing ; */
        debug("SQL -- Comment", yytext());
    }
    {LINETERM_MAC} {
        sqlCommand.append(yytext());
        if (sqlPrompt != null) psStd.print(sqlPrompt);
    }
    [^\"\';] {
        sqlCommand.append(yytext());
    }
    \' {
        /* TODO:  Find out if can escape " in SQL commands and handle! */
        sqlCommand.append(yytext());
        yybegin(SQL_SINGLE_QUOTED);
    }
    \" {
        /* TODO:  Find out if can escape " in SQL commands and handle! */
        sqlCommand.append(yytext());
        yybegin(SQL_DOUBLE_QUOTED);
    }
    ; {
        yybegin(YYINITIAL);
        return new Token(Token.SQL_TYPE, sqlCommand, yyline);
    }
}
<SQL_SINGLE_QUOTED> {
    [^\']+ {
        sqlCommand.append(yytext());
    }
    \'\' {
        sqlCommand.append(yytext());
    }
    \' {
        sqlCommand.append(yytext());
        debug("SQL '", yytext());
        yybegin(SQL);
    }
}
<SQL_DOUBLE_QUOTED> {
    [^\"]+ {
        sqlCommand.append(yytext());
    }
    \" {
        sqlCommand.append(yytext());
        yybegin(SQL);
        debug("SQL \"", yytext());
    }
}
[^\r\n] {
    yybegin(GOBBLE);
    return new Token(Token.SYNTAX_ERR_TYPE, yytext(), yyline);
}
