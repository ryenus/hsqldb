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


package org.hsqldb.lib;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.map.ValuePool;

/**
 * Uses a LineNumberReader and returns multiple consecutive lines which conform
 * to the specified group demarcation characteristics. Any exception
 * thrown while reading from the reader is handled internally.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 1.9.0
 */
public class LineGroupReader {

    private static final String[] defaultContinuations = new String[] {
        " ", "*"
    };
    private static final String[] defaultIgnoredStarts = new String[]{ "--" };
    static final String LS = System.getProperty("line.separator", "\n");

    //
    LineNumberReader reader;
    String           nextStartLine       = null;
    int              startLineNumber     = 0;
    int              nextStartLineNumber = 0;

    //
    final String[] sectionContinuations;
    final String[] sectionStarts;
    final String[] ignoredStarts;

    /**
     * Default constructor for TestUtil usage.
     * Sections start at lines beginning with any non-space character.
     * SQL comment lines are ignored.
     *
     * @param reader LineNumberReader
     */
    public LineGroupReader(LineNumberReader reader) {

        this.sectionContinuations = defaultContinuations;
        this.sectionStarts        = ValuePool.emptyStringArray;
        this.ignoredStarts        = defaultIgnoredStarts;
        this.reader               = reader;

        try {
            getNextSection();
        } catch (Exception e) {}
    }

    /**
     * Constructor for sections starting with specified strings.
     *
     * @param reader LineNumberReader
     * @param sectionStarts String[]
     */
    public LineGroupReader(LineNumberReader reader, String[] sectionStarts) {

        this.sectionStarts        = sectionStarts;
        this.sectionContinuations = ValuePool.emptyStringArray;
        this.ignoredStarts        = ValuePool.emptyStringArray;
        this.reader               = reader;

        try {
            getNextSection();
        } catch (Exception e) {}
    }

    public HsqlArrayList getNextSection() {

        String        line;
        HsqlArrayList list = new HsqlArrayList(new String[128], 0);

        if (nextStartLine != null) {
            list.add(nextStartLine);

            startLineNumber = nextStartLineNumber;
        }

        while (true) {
            boolean newSection = false;

            line = null;

            try {
                line = reader.readLine();
            } catch (Exception e) {}

            if (line == null) {
                nextStartLine = null;

                return list;
            }

            line = line.substring(0, StringUtil.rightTrimSize(line));

            //if the line is blank or a comment, then ignore it
            if (line.isEmpty() || isIgnoredLine(line)) {
                continue;
            }

            if (isNewSectionLine(line)) {
                newSection = true;
            }

            if (newSection) {
                nextStartLine       = line;
                nextStartLineNumber = reader.getLineNumber();

                return list;
            }

            list.add(line);
        }
    }

    public String getSectionAsString() {
        HsqlArrayList list = getNextSection();
        return convertToString(list, 0);
    }

    /**
     * Returns a map/list which contains the first line of each line group as
     * key and the rest of the lines as a String value.
     *
     * @return OrderedHashMap
     */
    public OrderedHashMap getAsMap() {

        OrderedHashMap map = new OrderedHashMap();

        while (true) {
            HsqlArrayList list = getNextSection();

            if (list.size() == 0) {
                break;
            }

            String key   = (String) list.get(0);
            String value = LineGroupReader.convertToString(list, 1);

            map.put(key, value);
        }

        return map;
    }

    private boolean isNewSectionLine(String line) {

        if (sectionStarts.length == 0) {
            for (int i = 0; i < sectionContinuations.length; i++) {
                if (line.startsWith(sectionContinuations[i])) {
                    return false;
                }
            }

            return true;
        } else {
            for (int i = 0; i < sectionStarts.length; i++) {
                if (line.startsWith(sectionStarts[i])) {
                    return true;
                }
            }

            return false;
        }
    }

    private boolean isIgnoredLine(String line) {

        for (int i = 0; i < ignoredStarts.length; i++) {
            if (line.startsWith(ignoredStarts[i])) {
                return true;
            }
        }

        return false;
    }

    public int getStartLineNumber() {
        return startLineNumber;
    }

    public void close() {

        try {
            reader.close();
        } catch (Exception e) {}
    }

    public static String convertToString(HsqlArrayList list, int offset) {

        StringBuilder sb = new StringBuilder();

        for (int i = offset; i < list.size(); i++) {
            sb.append(list.get(i)).append(LS);
        }

        return sb.toString();
    }

    public static OrderedHashMap getStatementMap(final String path) {

        OrderedHashMap  statementMap;
        String[]        starters = new String[]{ "/*" };
        LineGroupReader lg       = getGroupReader(path, starters);

        statementMap = lg.getAsMap();

        lg.close();

        return statementMap;
    }

    public static LineGroupReader getGroupReader(final String path,
            final String[] starters) {

        InputStream fis =
            AccessController.doPrivileged(new PrivilegedAction<InputStream>() {

            public InputStream run() {
                return getClass().getResourceAsStream(path);
            }
        });
        InputStreamReader reader = new InputStreamReader(fis,
            JavaSystem.CS_ISO_8859_1);
        LineNumberReader lineReader = new LineNumberReader(reader);

        return new LineGroupReader(lineReader, starters);
    }

    public static LineGroupReader getGroupReader(final String path) {
        return getGroupReader(path, ValuePool.emptyStringArray);
    }

}
