package org.hsqldb.util.sqltool;

public class Token {
    public static final int SQL_TYPE = 0;
    public static final int SPECIAL_TYPE = 1;
    public static final int PL_TYPE = 2;
    public static final int EDIT_TYPE = 3;
    public static final int RAW_TYPE = 4;
    public static final int RAWEXEC_TYPE = 5;
    public static final int SYNTAX_ERR_TYPE = 6;
    public static final int UNTERM_SQL_TYPE = 7;
    public static final int BUFFER_TYPE = 8;
    public int line;
    public TokenList nestedBlock = null;

    public String[] typeString = {
        "SQL", "SPECIAL", "PL", "EDIT", "RAW", "RAWEXEC", "SYNTAX",
        "UNTERM_SQL", "BUFFER"
    };

    public String val;
    public int type;
    public Token(int inType, String inVal, int inLine) {
        val = inVal; type = inType; line = inLine + 1;
    }
    public Token(int inType, StringBuffer inBuf, int inLine) {
        this(inType, inBuf.toString(), inLine);
    }
    public String toString() { return "@" + line
            + " TYPE=" + typeString[type] + ", VALUE=(" + val + ')';
    }
}
