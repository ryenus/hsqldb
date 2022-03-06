/* Copyright (c) 2001-2007, The HSQL Development Group
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

/*
 * $Id$
 */
/**
 * Preprocessor view of a line in a text document.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.6.2+
 * @since 1.8.1
 */
public class Line {

    /**
     * Is {@code "//#"}.
     */
    public static final String DIRECTIVE_PREFIX = "//#";
    /**
     * Is {@code " \t"}.
     */
    public static final String SPACE_CHARS = " \t";
    /**
     * Is {@code 3}.
     */
    public static final int DIRECTIVE_PREFIX_LENGTH = DIRECTIVE_PREFIX.length();
    /**
     * Is {@code 4}.
     */
    private static final int DIRECTIVE_PREFIX_LENGTH_PLUS_ONE
            = DIRECTIVE_PREFIX_LENGTH + 1;
    /**
     * Id {@code ' '}.
     */
    public static final String HIDE_DIRECTIVE = DIRECTIVE_PREFIX + ' ';

    /**
     * in the given line.
     *
     * @param line to inspect
     * @return the first index of any non-tab or non-space character.
     * @throws NullPointerException if line is {@code null}.
     */
    public static int indexOfNonTabOrSpace(final String line) {
        int pos = 0;
        int len = line.length();

        while (pos < len) {
            char ch = line.charAt(pos);

            if ((ch == ' ') || (ch == '\t')) {
                pos++;
                continue;
            }

            break;
        }

        return pos;
    }

    /**
     * in the given, generic string.
     *
     * @param string    to inspect
     * @param fromIndex at which to start the inspection; There is no
     *                  restriction on the value . If it is negative, it has the
     *                  same effect as if it were zero: the entire string may be
     *                  searched. If it is greater than the length of the string,
     *                  it has the same effect as if it were equal to the length
     *                  of string: {@code  -1} is returned.
     * @return the first index of either a tab ort a space character; {@code -1}
     *         if no such characters are found.
     * @throws NullPointerException if string is {@code null}.
     */
    public static int indexOfTabOrSpace(final String string, final int fromIndex) {
        final int spos = string.indexOf(' ', fromIndex);
        final int tpos = string.indexOf('\t', fromIndex);

        return (((tpos != -1) && (tpos < spos)) || (spos == -1)) ? tpos : spos;
    }

    private int type;
    private String sourceText;
    private String indent;
    private String text;
    private String arguments;

    /**
     * Constructs a new instance from the given line.
     *
     * @param line in a Document being Preprocessed.
     * @throws PreprocessorException
     * @throws NullPointerException
     */
    public Line(String line) throws PreprocessorException, NullPointerException {
        final Line target = this;
        target.setSourceText(line);
    }

    public void setSourceText(String line) throws PreprocessorException, NullPointerException {
        this.sourceText = line;
        int pos = indexOfNonTabOrSpace(line);
        this.indent = line.substring(0, pos);
        final String subline = line.substring(pos);

        if (!subline.startsWith(DIRECTIVE_PREFIX)) {
            this.text = subline;
            this.arguments = null;
            this.type = LineType.VISIBLE;
        } else if (subline.length() == DIRECTIVE_PREFIX_LENGTH) {
            this.text = "";
            this.arguments = null;
            this.type = LineType.HIDDEN;
        } else if (SPACE_CHARS.indexOf(subline.charAt(DIRECTIVE_PREFIX_LENGTH)) != -1) {
            this.text = subline.substring(DIRECTIVE_PREFIX_LENGTH_PLUS_ONE);
            this.arguments = null;
            this.type = LineType.HIDDEN;
        } else {
            pos = indexOfTabOrSpace(subline, DIRECTIVE_PREFIX_LENGTH_PLUS_ONE);
            if (pos == -1) {
                this.text = subline;
                this.arguments = null;
            } else {
                this.text = subline.substring(0, pos);
                this.arguments = subline.substring(pos + 1).trim();
            }
            final Integer typeId = LineType.id(text);
            if (typeId == null) {
                throw new PreprocessorException("Unknown directive ["
                        + text + "] in [" + subline + "]"); // NOI18N
            }
            this.type = typeId;
        }

    }

    /**
     * for this line, if they exist.
     *
     * @return the value.
     * @throws PreprocessorException if the line has no arguments.
     */
    public String getArguments() throws PreprocessorException {
        if (arguments == null || arguments.isEmpty()) {
            throw new PreprocessorException("[" + text
                    + "]: has no argument(s)"); // NOI18N
        }
        return arguments;
    }

    /**
     * which is the original, verbatim value with which this instance was
     * constructed.
     *
     * @return the source text val
     */
    public String getSourceText() {
        return sourceText;
    }

    /**
     * prefix for this instance, which is any leading space or table characters.
     *
     * @return the leading space or table characters for this instance.
     */
    public String getIndent() {
        return indent;
    }

    /**
     * which is not the directive or directive arguments portion of the line.
     * <p>
     * For visible lines, this is the portion following the indent, if any.
     * </p>
     * <p>
     * For hidden lines, this is the text, if any, following the directive
     * prefix used to hide the line.
     * </p>
     *
     * @return
     */
    public String getText() {
        return text;
    }

    public int getType() {
        return type;
    }

    public boolean isType(int lineType) {
        return (this.type == lineType);
    }

    @Override
    public String toString() {
        return "" + LineType.label(this.type) + "(" + this.type + "): indent ["
                + this.indent + "] text [" + this.text
                + ((this.arguments == null) ? "]" : ("] args ["
                        + this.arguments + "]"));
    }
}
