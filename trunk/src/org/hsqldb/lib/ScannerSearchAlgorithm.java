/* Copyright (c) 2001-2024, The HSQL Development Group
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


package org.hsqldb.lib;

import java.io.Reader;

import java.util.Scanner;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Provides a string search facility using a {@link Scanner}.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.x
 * @since  2.7.x
 */
public class ScannerSearchAlgorithm {

    /**
     * is 1024. This is the size above which it <i>may</i> be better to use
     * {@link KMPSearchAlgorithm}.
     */
    public static final int SUGGESTED_MAX_LITERAL_SIZE = 1024;
    private static final Logger LOG = Logger.getLogger(
        ScannerSearchAlgorithm.class.getName());

    /**
     * the given {@code reader} for the given {@code searchstr}.
     *
     * @param reader    to search
     * @param searchstr to find
     * @param literal   true to treat {@code searchstr} as a literal search
     *                  term; false to treat {@code searchstr} as a regular
     *                  expression.
     * @return zero-based offset into stream at which {@code searchstr} is
     *         found; -1 if not found, {@code reader} is null, or
     *         {@code searchstr} is null; 0 if {@code searchstr.length() == 0
     *         && literal == true}.
     * @throws IllegalStateException    if a {@link Scanner} illegal state
     *                                  occurs
     * @throws IllegalArgumentException if a {@link Scanner} illegal argument is
     *                                  encountered.
     */
    public static long search(
            Reader reader,
            char[] searchstr,
            boolean literal) {

        if (reader == null || searchstr == null) {
            return -1;
        }

        if (searchstr.length == 0 && literal) {
            return 0;
        }

        return search(reader, new String(searchstr), literal);
    }

    /**
     * the given {@code reader} for the given {@code searchstr}.
     *
     * @param reader    to search
     * @param searchstr to find
     * @param literal   true to treat {@code searchstr} as a literal search
     *                  term; false to treat {@code searchstr} as a regular
     *                  expression.
     * @return zero-based offset into stream at which {@code searchstr} is
     *         found; -1 if not found, {@code reader} is null, or
     *         {@code searchstr} is null; 0 if {@code searchstr.length() == 0
     *         && literal == true}.
     * @throws PatternSyntaxException   if {@code searchstr} expression's syntax
     *                                  is invalid
     * @throws IllegalStateException    if a {@link Scanner} illegal state
     *                                  occurs
     * @throws IllegalArgumentException if a {@link Scanner} illegal argument is
     *                                  encountered.
     */
    public static long search(
            Reader reader,
            String searchstr,
            boolean literal)
            throws NullPointerException,
                   PatternSyntaxException,
                   IllegalStateException,
                   IllegalArgumentException {

        if (reader == null || searchstr == null) {
            return -1;
        }

        if (searchstr.length() == 0 && literal) {
            return 0;
        }

        final String  s       = literal
                                ? Pattern.quote(searchstr)
                                : searchstr;
        final Pattern pattern = Pattern.compile(s);

        return searchNoChecks(reader, pattern);
    }

    /**
     * the given {@code reader} for the given {@code pattern}.
     *
     * @param reader  to search
     * @param pattern to find
     * @return zero-based offset into stream at which {@code searchstr} is
     *         found; -1 if not found;
     * @throws NullPointerException     if {@code reader} is null or
     *                                  {@code pattern} is null.
     * @throws IllegalStateException    if a {@link Scanner} illegal state
     *                                  occurs
     * @throws IllegalArgumentException if a {@link Scanner} illegal argument is
     *                                  encountered.
     */
    public static long search(
            Reader reader,
            Pattern pattern)
            throws IllegalStateException,
                   IllegalArgumentException {

        if (reader == null || pattern == null) {
            return -1;
        }

        return searchNoChecks(reader, pattern);
    }

    /**
     * the given {@code reader} for the given {@code pattern}.
     *
     * @param reader  to search
     * @param pattern to find
     * @return zero-based offset into stream at which {@code searchstr} is
     *         found; -1 if not found;
     * @throws IllegalStateException    if a {@link Scanner} illegal state
     *                                  occurs
     * @throws IllegalArgumentException if a {@link Scanner} illegal argument is
     *                                  encountered.
     */
    private static long searchNoChecks(
            Reader reader,
            Pattern pattern)
            throws IllegalStateException,
                   IllegalArgumentException {

        final Scanner scanner  = new Scanner(reader);
        final String  token    = scanner.findWithinHorizon(pattern, 0);
        final long    position = (token == null)
                                 ? -1
                                 : scanner.match().start();

        return position;
    }

    private ScannerSearchAlgorithm() {
        assert false: "Pure Utility Class";
    }
}
