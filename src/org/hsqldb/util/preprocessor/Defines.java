/* Copyright (c) 2001-2022, The HSQL Development Group
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


package org.hsqldb.util.preprocessor;

import java.util.HashMap;
import java.util.Map;

/*
 * $Id$
 */
/**
 * A simple Preprocessor symbol table.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.8.1
 */
@SuppressWarnings("ClassWithoutLogger")
public class Defines {

    private final Map<String, Object> symbols;

    /**
     * Constructs a new, empty instance.
     */
    public Defines() {
        this.symbols = new HashMap<>(16);
    }

    /**
     * Constructs a new instance initially defined using the comma-separated
     * list of Preprocessor expressions.
     *
     * @param csvExpressions a comma-separated list of Preprocessor expressions;
     *                       may be {@code null} or empty, in which case no
     *                       symbols are added.
     * @throws PreprocessorException if the given list contains an illegal
     *                               expression.
     * @see #defineCSV(String);
     */
    public Defines(final String csvExpressions) throws PreprocessorException {
        this();
        if (csvExpressions != null && !csvExpressions.isEmpty()) {
            final Defines target = this;
            target.defineCSV(csvExpressions);
        }
    }

    /**
     * removes all defined symbols.
     */
    public void clear() {
        this.symbols.clear();
    }

    /**
     * adds the given comma-separated list of Preprocessor expressions to the
     * symbol table.
     *
     * @param csvExpressions a comma-separated list of Preprocessor expressions;
     *                       may be {@code null} or empty, in which case no
     *                       symbols are added.
     * @throws PreprocessorException if the given list contains an illegal
     *                               expression.
     */
    public void defineCSV(final String csvExpressions)
            throws PreprocessorException {
        if (csvExpressions == null || csvExpressions.isEmpty()) {
            return;
        }
        final String tce = csvExpressions.trim() + ',';
        int start = 0;
        int len = tce.length();
        while (start < len) {
            final int end = tce.indexOf(',', start);
            final String expr = tce.substring(start, end).trim();
            if (!expr.isEmpty()) {
                defineSingle(expr);
            }
            start = end + 1;
        }
    }

    /**
     * adds a single Preprocessor expression to the symbol table.
     *
     * @param expression to add; may be {@code null} or empty, in which case no
     *                   action is taken.
     * @throws PreprocessorException if the given expression is illegal or has
     *                               trailing tokens.
     */
    public void defineSingle(final String expression) throws PreprocessorException {
        if (expression == null || expression.isEmpty()) {
            return;
        }
        final Tokenizer tokenizer = new Tokenizer(expression);
        tokenizer.next();
        if (!tokenizer.isToken(Token.IDENT)) {
            throw new PreprocessorException("IDENT token required at position: "
                    + tokenizer.getStartIndex()
                    + " in ["
                    + expression
                    + "]"); // NOI18N
        }

        final String ident = tokenizer.getIdent();

        int tokenType = tokenizer.next();

        switch (tokenType) {
            case Token.EOI: {
                this.symbols.put(ident, ident);
                return;
            }
            case Token.ASSIGN: {
                tokenType = tokenizer.next();
                break;
            }
            default: {
                break;
            }
        }

        switch (tokenType) {
            case Token.NUMBER: {
                final Number number = tokenizer.getNumber();

                this.symbols.put(ident, number);

                break;
            }
            case Token.STRING: {
                final String string = tokenizer.getString();

                this.symbols.put(ident, string);

                break;
            }
            case Token.IDENT: {
                final String rhsIdent = tokenizer.getIdent();

                if (!isDefined(rhsIdent)) {
                    throw new PreprocessorException("Right hand side"
                            + "IDENT token [" + rhsIdent + "] at position: "
                            + tokenizer.getStartIndex()
                            + " is undefined in ["
                            + expression
                            + "]"); // NOI18N
                }

                final Object value = this.symbols.get(rhsIdent);

                symbols.put(ident, value);
                break;
            }
            default: {
                throw new PreprocessorException("Right hand side NUMBER,"
                        + "STRING or IDENT token required at position: "
                        + +tokenizer.getStartIndex()
                        + " in ["
                        + expression
                        + "]"); // NOI18N
            }
        }

        tokenizer.next();

        if (!tokenizer.isToken(Token.EOI)) {
            throw new PreprocessorException("Illegal trailing "
                    + "characters at position: "
                    + tokenizer.getStartIndex()
                    + " in ["
                    + expression
                    + "]"); // NOI18N
        }
    }

    public void undefine(final String symbol) {
        this.symbols.remove(symbol);
    }

    public boolean isDefined(final String symbol) {
        return symbol == null || symbol.isEmpty()
                ? false
                : this.symbols.containsKey(symbol);
    }

    public Object getDefintion(final String symbol) {
        return symbol == null || symbol.isEmpty()
                ? null
                : this.symbols.get(symbol);
    }

    public boolean evaluate(final String expression) throws PreprocessorException {
        if (expression == null || expression.isEmpty()) {
            return false;
        }
        final Tokenizer tokenizer = new Tokenizer(expression);

        tokenizer.next();

        final Parser parser = new Parser(this, tokenizer);
        final boolean result = parser.parseExpression();

        if (!tokenizer.isToken(Token.EOI)) {
            throw new PreprocessorException("Illegal trailing "
                    + "characters at position: "
                    + tokenizer.getStartIndex()
                    + " in ["
                    + expression
                    + "]"); // NOI18N
        }

        return result;
    }

    @Override
    public String toString() {
        return super.toString() + this.symbols.toString();
    }
}
