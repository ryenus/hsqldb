package org.hsqldb.util.sqltool;

import java.io.IOException;

public interface TokenSource {
    public Token yylex() throws IOException;
}
