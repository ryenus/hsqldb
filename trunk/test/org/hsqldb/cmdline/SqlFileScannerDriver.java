package org.hsqldb.cmdline;

import java.io.FileReader;
import org.hsqldb.util.sqltool.SqlFileScanner;
import org.hsqldb.util.sqltool.Token;
import org.hsqldb.util.sqltool.TokenSource;
import org.hsqldb.util.sqltool.TokenList;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Exercises the SqlFile Scanner.
 *
 * Can be used to test scanning behavior for complex cases like nesting,
 * either interactively or with SQL scripts.
 */
public class SqlFileScannerDriver {
    static Pattern foreachPattern =
            Pattern.compile("(?i)foreach\\s+(\\S+)\\s*\\(\\s*([^)]+)*\\)\\s*");
    static public void main(String[] sa) throws IOException {
        if (sa.length > 1) {
            System.err.println("SYNTAX:  java SqlFileScannerDriver [filename]");
            System.exit(2);
        }
        // Useful only for int type Tokens
        //System.err.println("YYEOF = '" + SqlFileScanner.YYEOF + "'");
        SqlFileScanner y = ((sa.length > 0)
                ? new SqlFileScanner(new FileReader(sa[0]))
                : new SqlFileScanner(System.in)
        );
        if (sa.length < 1) {
            y.setRawPrompt("raw> ");
            y.setSqlPrompt("+sql> ");
            y.setSqltoolPrompt("sql> ");
            y.setInteractive(true);
            System.out.print("sql> ");
        }

        new SqlFileScannerDriver(y);
        System.exit(0);
    }

    private SqlFileScanner y;

    public SqlFileScannerDriver(SqlFileScanner y) throws IOException {
        this.y = y;
        process(y);
    }

    /**
     * Handles each command Token as follows.
     * <UL>
     *   <LI>Execute each non-nested command
     *   <LI>Parse each non-parsed nested comand
     *   <LI>Execute each nested command (they are all parsed at this point).
     * </UL>
     */
    private void process(TokenSource ts) throws IOException {
        Token token;
        while ((token = ts.yylex()) != null) {
            if (!isNestingCommand(token)) {
                System.out.println(token.toString());
                if (token.type == Token.SPECIAL_TYPE &&
                    token.val.trim().equalsIgnoreCase("q")) break;
                continue;
            }
            if (token.nestedBlock == null)
                token.nestedBlock = parseBlock(token.val);
            processBlock(token);
        }
    }

    private boolean isNestingCommand(Token token) {
        if (token.type != Token.PL_TYPE) return false;
        if (token.val.trim().startsWith("foreach")) return true;
        return false;
    }

    /**
     * Parses a block command like foreach/if.
     */
    private TokenList parseBlock(String command) throws IOException {
        if (command.trim().startsWith("foreach")) return seekTokenSource("end");
        throw new RuntimeException("Can't parse block for unexpected command: "
                + command);
    }

    private void processBlock(Token token) throws IOException {
        Matcher matcher = foreachPattern.matcher(token.val);
        if (matcher.matches()) {
            //for (String v : matcher.group(2).split("\\s+")) {
            // Java5
            String[] vals = matcher.group(2).split("\\s+");
            for (int i = 0; i < vals.length; i++) {
                System.err.println("BLOCK w/ " + matcher.group(1) + "="
                        + vals[i]);
                process(token.nestedBlock.dup());
            }
        } else {
            throw new RuntimeException("Can't process unknown block command: "
                    + token.val);
        }
    }

    private TokenList seekTokenSource(String endToken) throws IOException {
        Token token; TokenList newTS = new TokenList();
        while ((token = y.yylex()) != null) {
            System.err.print("LOADING ");
            System.err.println(token.toString());
            if (token.type == Token.PL_TYPE
                    && token.val.matches("\\s*(?i)" + endToken + "\\s*")) {
                System.err.println("Terminated block with " + token.val);
                return newTS;
            }
            if (isNestingCommand(token)) 
                token.nestedBlock = parseBlock(token.val);
            newTS.add(token);
        }
        throw new IOException("Unterminated block at line " + token.line);
    }
}
