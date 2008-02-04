package org.hsqldb.util.sqltool;

public class Token {
    public static final int SQL_TYPE = 0;
    public static final int SPECIAL_TYPE = 1;
    public static final int PL_TYPE = 2;
    public static final int EDIT_TYPE = 3;
    public static final int RAW_TYPE = 4;
    public static final int RAWEXEC_TYPE = 5;
    public static final int SYNTAX_ERR_TYPE = 6;
    public static final int UNTERM_TYPE = 7;
    public static final int BUFFER_TYPE = 8;
    public int line;
    public TokenList nestedBlock = null;

    public String[] typeString = {
        "SQL", "SPECIAL", "PL", "EDIT", "RAW", "RAWEXEC", "SYNTAX",
        "UNTERM", "BUFFER"
    };

    public String val;
    public int type;
    public Token(int inType, String inVal, int inLine) {
        val = inVal; type = inType; line = inLine + 1;
        switch (inType) {
            case SPECIAL_TYPE:
            case EDIT_TYPE:
            case PL_TYPE:
                // These types must be not null.  May be just whitespace.
                // Will be trimmed.
                if (val == null) throw new IllegalArgumentException(
                        "Null String value for scanner token");
                if (val.length() > 0 && (val.charAt(val.length() - 1) == ' '
                    || val.charAt(val.length() - 1) == '\t'
                    || val.charAt(val.length() - 1) == '\f'
                    || val.charAt(val.length() - 1) == '\n'
                    || val.charAt(val.length() - 1) == '\r'))
                val = val.trim();
                break;

            case SYNTAX_ERR_TYPE:
            case BUFFER_TYPE:
            case RAW_TYPE:
            case RAWEXEC_TYPE:
            case UNTERM_TYPE:
                // These types must be not null.  May be just whitespace.
                // Will NOT be trimmed.
                if (val == null) throw new IllegalArgumentException(
                        "Null String value for scanner token");
                break;

            case SQL_TYPE:
                // These types may be anything (null, whitespace, etc.).
                // Will NOT be trimmed
                break;

            default: throw new IllegalArgumentException(
                "Internal error.  Unexpected scanner token type: " + inType);
        }
    }

    public Token(int inType, StringBuffer inBuf, int inLine) {
        this(inType, inBuf.toString(), inLine);
    }

    public Token(int inType, int inLine) {
        this(inType, (String) null, inLine);
    }

    public String toString() { return "@" + line
            + " TYPE=" + typeString[type] + ", VALUE=(" + val + ')';
    }
}
