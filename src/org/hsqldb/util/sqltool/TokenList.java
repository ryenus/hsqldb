package org.hsqldb.util.sqltool;

import java.util.ArrayList;

public class TokenList extends ArrayList implements TokenSource {
// public class TokenList extends ArrayList<Token> implements TokenSource {
// Java 5
    public TokenList() {
        super();
    }
    public TokenList(TokenList inList) {
        super(inList);
    }
    public Token yylex() {
        if (size() < 1) return null;
        //return remove(0);
        // Java5
        return (Token) remove(0);
    }

    public TokenList dup() {
        return new TokenList(this);
    }
}
