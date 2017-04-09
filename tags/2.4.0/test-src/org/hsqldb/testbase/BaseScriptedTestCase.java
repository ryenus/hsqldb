/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb.testbase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.hsqldb.lib.StringUtil;

/**
 * Initial port of org.hqldb.test.TestUtil.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public abstract class BaseScriptedTestCase extends BaseTestCase {

    // for subclasses
    protected BaseScriptedTestCase() {
        super();
    }

    /**
     *
     * @param name of script resource.
     */
    public BaseScriptedTestCase(String name) {
        super(name);
    }

    /**
     *
     * @throws java.lang.Exception
     * @return
     */
    protected Statement newStatement() throws Exception {
        return connectionFactory().createStatement(newConnection());
    }

    /**
     *
     * @throws java.lang.Throwable
     */
    @Override
    public void runTest() throws Throwable {
        try {
            executeScript(getName());
        } catch (Throwable t) {
            this.printException(t);
            throw t;
        }
    }

    /**
     *
     * @param resource
     * @throws java.lang.Exception
     * @return
     */
    protected Reader getScriptReader(final String resource) throws Exception {

        final URL resourceURL = getClass().getResource(resource);

        if (resourceURL == null) {
            throw new IOException("No such resource: " + resource);
        }

        return new InputStreamReader(resourceURL.openStream());
    }

    /**
     *
     * @param resource
     * @throws java.lang.Exception
     */
    @Override
    protected void executeScript(String resource) throws Exception {

        List section = null;
        Statement statement = newStatement();
        LineNumberReader reader = new LineNumberReader(
                getScriptReader(resource));

        printProgress("Opened test script: " + resource);

        int startLine = 1;

        while (true) {
            boolean startSection = false;
            String line = reader.readLine();

            if (line == null) {
                break;
            }

            line = line.substring(0, StringUtil.rightTrimSize(line));

            if ((line.length() == 0) || line.startsWith("--")) {
                continue;
            }

            if (line.startsWith("/*")) {
                startSection = true;
            }

            if (line.charAt(0) != ' ' && line.charAt(0) != '*') {
                startSection = true;
            }

            if (startSection) {
                if (section != null) {
                    executeSection(statement, section, startLine);
                }

                section = new ArrayList();
                startLine = reader.getLineNumber();
            }

            section.add(line);
        }

        if (section != null) {
            executeSection(statement, section, startLine);
        }

        statement.close();
        printProgress("Processed lines: " + reader.getLineNumber());
    }

    /**
     *
     * @param stmt
     * @param lines
     * @param line
     */
    protected void executeSection(Statement stmt, List lines, int line) {
        BaseSection section = getSectionFactory().createSection(lines);

        if (section == null) {
            //it was not possible to sucessfully parse the section
            printProgress("The section starting at line "
                    + line
                    + " could not be parsed " + "and was not processed");
        } else if (section instanceof IgnoredSection) {
            printProgress("Line " + line + ": " + section.getResultString());
        } else if (section instanceof DisplaySection) {
            printProgress(section.getResultString());
        } else if (!section.execute(stmt)) {
            printProgress("section starting at line " + line);
            printProgress("returned an unexpected result.");
            //println(section.toString());
            TestCase.fail(section.toString());
        }
    }
    /**
     *
     */
    private SectionFactory m_sectionFactory;

    /**
     *
     * @return
     */
    protected SectionFactory getSectionFactory() {

        if (m_sectionFactory == null) {
            String factoryClass = getProperty("SectionFactory", null);

            if (factoryClass == null) {
                m_sectionFactory = new DefaultSectionFactory();
            } else {
                try {
                    m_sectionFactory = (SectionFactory) Class.forName(
                            factoryClass).newInstance();
                } catch (Exception ex) {
                    printException(ex);

                    m_sectionFactory = new DefaultSectionFactory();
                }
            }
        }

        return m_sectionFactory;
    }

    /**
     *
     */
    protected interface SectionFactory {

        /**
         * Factory method to create appropriate parsed section class
         * for a section.
         *
         *
         * @return a ParesedSection object
         * @param lines
         */
        BaseSection createSection(List lines);
    }

    /**
     *
     */
    protected class DefaultSectionFactory implements SectionFactory {

        /**
         *
         * @param list
         * @return
         */
        public BaseSection createSection(List list) {
            char sectionType = ' ';
            String[] lines = null;
            String firstLine = (String) list.get(0);

            if (firstLine.startsWith("/*")) {
                sectionType = firstLine.charAt(2);

                if (!isRecognizedSectionType(sectionType)) {
                    return null;
                }

                if ((Character.isUpperCase(sectionType)) &&
                        (getBooleanProperty("IgnoreCodeCase", true))) {
                    sectionType = Character.toLowerCase(sectionType);
                }

                firstLine = firstLine.substring(3);
            }

            int offset = 0;

            if (firstLine.trim().length() > 0) {
                lines = new String[list.size()];
                lines[0] = firstLine;
            } else {
                lines = new String[list.size() - 1];
                offset = 1;
            }

            for (int i = (1 - offset); i < lines.length; i++) {
                lines[i] = (String) list.get(i + offset);
            }

            switch (sectionType) {

                case 'u': {
                    return new UpdateCountSection(lines);
                }
                case 's': {
                    return new SilentSection(lines);
                }
                case 'r': {
                    return new ResultSetSection(lines);
                }
                case 'c': {
                    return new RowCountSection(lines);
                }
                case 'd': {
                    return new DisplaySection(lines);
                }
                case 'e': {
                    return new ExceptionSection(lines);
                }
                case ' ': {
                    return new BlankSection(lines);
                }
                default: {
                    return new IgnoredSection(lines, sectionType);
                }
            }
        }

        /**
         *
         * @param sectionType
         * @return
         */
        protected boolean isRecognizedSectionType(char sectionType) {
            switch (Character.toLowerCase(sectionType)) {
                case ' ':
                case 'r':
                case 'e':
                case 'c':
                case 'u':
                case 's':
                case 'd': {
                    return true;
                }
                default: {
                    return false;
                }
            }
        }
    }

    /**
     *
     */
    protected abstract class BaseSection {

        /**
         *
         */
        protected char m_type = ' ';
        /**
         *
         */
        String m_message = null;
        /**
         *
         */
        protected String[] m_lines = null;
        /**
         *
         */
        protected int m_resEndRow = 0;
        /**
         *
         */
        protected String m_sql = null;

        /**
         *
         */
        protected BaseSection() {
        }

        /**
         *
         * @param lines
         */
        protected BaseSection(String[] lines) {

            m_lines = lines;

            StringBuilder sb = new StringBuilder();
            int endIndex = 0;
            int k = m_lines.length - 1;

            do {
                if ((endIndex = m_lines[k].indexOf("*/")) != -1) {

                    sb.insert(0, " ");
                    sb.insert(0, m_lines[k].substring(endIndex + 2));

                    m_lines[k] = m_lines[k].substring(0, endIndex);

                    if (m_lines[k].length() == 0) {
                        m_resEndRow = k - 1;
                    } else {
                        m_resEndRow = k;
                    }

                    break;
                } else {
                    sb.insert(0, " ");
                    sb.insert(0, m_lines[k]);
                }

                k--;
            } while (k >= 0);

            m_sql = sb.toString();
        }

        /**
         *
         * @return
         */
        @Override
        public String toString() {
            String className = getClass().getName();
            int lastDot = className.lastIndexOf('.');
            String simpleName = (lastDot >= 0)
                    ? className.substring(lastDot + 1)
                    : className;
            char type = getType();
            String sectionType = (type == ' ') ? simpleName
                    : type + ": " + simpleName;
            StringBuilder sb = new StringBuilder();

            if (getMessage() != null) {
                sb.append('\n');
                sb.append(getMessage());
            }
            sb.append("\n");
            sb.append("******\n");
            sb.append("\n");
            sb.append("Section Type    : ").append(sectionType).append('\n');
            sb.append("Section Result  : ").append(getResultString()).append('\n');
            sb.append("Section Content :\n");

            for (int i = 0; i < m_lines.length; i++) {
                if (m_lines[i].trim().length() > 0) {
                    sb.append(MessageFormat.format(
                            "line {0}: {1}\n",
                            new Object[]{
                                "" + i,
                                m_lines[i]
                            }));
                }
            }

            sb.append("Submitted SQL   : \n");
            sb.append(getSql()).append('\n');
            sb.append("\n");
            sb.append("******\n");

            return sb.toString();
        }

        /**
         *
         * @return
         */
        protected abstract String getResultString();

        /**
         *
         * @return
         */
        protected String getMessage() {
            return m_message;
        }

        /**
         *
         * @return
         */
        protected char getType() {
            return m_type;
        }

        /**
         *
         * @return
         */
        protected String getSql() {
            return m_sql;
        }

        /**
         *
         * @param stmt
         * @return
         */
        protected boolean execute(Statement stmt) {
            boolean success = false;

            try {
                stmt.execute(getSql());
                success = true;
            } catch (Exception x) {
                java.io.StringWriter sw = new java.io.StringWriter();
                java.io.PrintWriter pw = new java.io.PrintWriter(sw);
                x.printStackTrace(pw);
                m_message = sw.toString();
            }

            return success;
        }
    }

    /**
     *
     */
    protected class ResultSetSection extends BaseSection {

        /**
         *
         */
        private String m_delimiter = getProperty("TestUtilFieldDelimiter", ",");
        /**
         *
         */
        private String[] m_expectedRows = null;

        /**
         *
         * @param lines String[]
         */
        protected ResultSetSection(String[] lines) {

            super(lines);

            m_type = 'r';

            m_expectedRows = new String[(m_resEndRow + 1)];

            for (int i = 0; i <= m_resEndRow; i++) {
                int skip = StringUtil.skipSpaces(lines[i], 0);

                m_expectedRows[i] = lines[i].substring(skip);
            }
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {

            final StringBuffer sb = new StringBuffer();
            final String[] expectedRows = getExpectedRows();
            final int len = expectedRows.length;

            for (int i = 0; i < len; i++) {
                sb.append(expectedRows[i]).append('\n');
            }

            return sb.toString();
        }

        protected String getTypeName() {
            return "Result Set Section";
        }

        /**
         *
         * @param stmt
         * @return
         */
        @Override
        protected boolean execute(Statement stmt) {

            try {
                try {
                    stmt.execute(getSql());
                } catch (SQLException s) {
                    throw new Exception(
                            "Expected a ResultSet, but got the error: " + s.toString());
                }

                //check that update count != -1
                if (stmt.getUpdateCount() != -1) {
                    throw new Exception(
                            "Expected a ResultSet, but got an update count of " + stmt.getUpdateCount());
                }

                //iterate over the ResultSet
                ResultSet results = stmt.getResultSet();
                int count = 0;

                while (results.next()) {
                    if (count < getExpectedRows().length) {
                        String[] expectedFields =
                                StringUtil.split(getExpectedRows()[count], m_delimiter);

                        if (results.getMetaData().getColumnCount() == expectedFields.length) {

                            int j = 0;

                            for (int i = 0; i < expectedFields.length; i++) {
                                j = i + 1;

                                String actual = results.getString(j);

                                if (actual == null) {

                                    if (!expectedFields[i].equalsIgnoreCase(
                                            "NULL")) {
                                        throw new Exception(
                                                "Expected row " + count + " of the ResultSet to contain:\n" + getExpectedRows()[count] + "\nbut field " + j + " contained NULL");
                                    }
                                } else if (!actual.equals(expectedFields[i])) {

                                    //then the results are different
                                    throw new Exception(
                                            "Expected row " + (count + 1) + " of the ResultSet to contain:\n" + getExpectedRows()[count] + "\nbut field " + j + " contained " + results.getString(j));
                                }
                            }
                        } else {

                            //we have the wrong number of columns
                            throw new Exception(
                                    "Expected the ResultSet to contain " + expectedFields.length + " fields, but it contained " + results.getMetaData().getColumnCount() + " fields.");
                        }
                    }

                    count++;
                }

                if (count != getExpectedRows().length) {

                    throw new Exception("Expected the ResultSet to contain " + getExpectedRows().length + " rows, but it contained " + count + " rows.");
                }
            } catch (Exception x) {
                m_message = x.getMessage();

                return false;
            }

            return true;
        }

        /**
         *
         * @return
         */
        private String[] getExpectedRows() {
            return m_expectedRows;
        }
    }

    /**
     *
     */
    protected class UpdateCountSection extends BaseSection {

        /**
         *
         */
        int m_expectedUpdateCount;

        /**
         *
         * @param lines
         */
        protected UpdateCountSection(String[] lines) {
            super(lines);

            m_type = 'u';
            m_expectedUpdateCount = Integer.parseInt(lines[0]);
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {
            return Integer.toString(getExpectedUpdateCount());
        }

        /**
         *
         * @return
         */
        private int getExpectedUpdateCount() {
            return m_expectedUpdateCount;
        }

        /**
         *
         * @param stmt
         * @return
         */
        @Override
        protected boolean execute(final Statement stmt) {
            try {
                try {
                    stmt.execute(getSql());
                } catch (SQLException se) {
                    throw new Exception("Expected an update count of " + getExpectedUpdateCount() + ", but got the error: " + se.toString());
                }

                if (stmt.getUpdateCount() != getExpectedUpdateCount()) {
                    throw new Exception("Expected an update count of " + getExpectedUpdateCount() + ", but got an update count of " + stmt.getUpdateCount() + ".");
                }
            } catch (Exception ex) {
                m_message = ex.getMessage();

                return false;
            }

            return true;
        }
    }

    /**
     *
     */
    protected class SilentSection extends BaseSection {

        /**
         *
         * @param lines
         */
        protected SilentSection(String[] lines) {
            super(lines);

            m_type = 's';
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {
            return null;
        }

        /**
         *
         * @param stmt
         * @return
         */
        @Override
        protected boolean execute(Statement stmt) {
            try {
                stmt.execute(getSql());
            } catch (Exception x) {
            }

            return true;
        }
    }

    /**
     *
     */
    protected class RowCountSection extends BaseSection {

        /**
         *
         */
        private int m_expectedRowCount;

        /**
         *
         * @param lines
         */
        protected RowCountSection(String[] lines) {
            super(lines);

            m_type = 'c';
            m_expectedRowCount = Integer.parseInt(lines[0]);
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {
            return Integer.toString(getExpectedRowCount());
        }

        /**
         *
         * @return
         */
        private int getExpectedRowCount() {
            return m_expectedRowCount;
        }

        /**
         *
         * @param stmt
         * @return
         */
        @Override
        protected boolean execute(Statement stmt) {
            try {
                try {
                    stmt.execute(getSql());
                } catch (SQLException se) {
                    throw new Exception(
                            "Expected a ResultSet containing "
                            + getExpectedRowCount()
                            + " rows, but got an error: "
                            + se.toString());
                }

                if (stmt.getUpdateCount() != -1) {
                    throw new Exception(
                            "Expected a ResultSet, but got an update count of "
                            + stmt.getUpdateCount());
                }

                ResultSet results = stmt.getResultSet();
                int count = 0;

                while (results.next()) {
                    count++;
                }

                if (count != getExpectedRowCount()) {

                    throw new Exception(
                            "Expected the ResultSet to contain "
                            + getExpectedRowCount()
                            + " rows, but it contained "
                            + count
                            + " rows.");
                }
            } catch (Exception ex) {
                m_message = ex.getMessage();

                return false;
            }

            return true;
        }
    }

    /**
     *
     */
    protected class ExceptionSection extends BaseSection {

        /**
         *
         * @param lines
         */
        protected ExceptionSection(String[] lines) {
            super(lines);

            m_type = 'e';
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {
            return "SQLException";
        }

        /**
         *
         * @param aStatement
         * @return
         */
        @Override
        protected boolean execute(Statement stmt) {

            try {
                stmt.execute(getSql());
            } catch (SQLException se) {
                return true;
            } catch (Exception ex) {
                m_message = ex.getMessage();

                return false;
            }

            return false;
        }
    }

    /**
     *
     */
    protected class BlankSection extends BaseSection {

        /**
         *
         * @param lines
         */
        protected BlankSection(String[] lines) {
            super(lines);

            m_type = ' ';
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {
            return "No result specified for this section";
        }
    }

    /**
     *
     */
    protected class IgnoredSection extends BaseSection {

        /**
         *
         * @param lines
         * @param type
         */
        protected IgnoredSection(String[] lines, char type) {
            super(lines);

            m_type = type;
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {
            return "This section, of type '" + getType() + "' was ignored";
        }
    }

    /**
     *
     */
    protected class DisplaySection extends BaseSection {

        /**
         *
         * @param inLines
         */
        protected DisplaySection(String[] lines) {
            m_lines = lines;
            int firstSlash = m_lines[0].indexOf('/');
            m_lines[0] = m_lines[0].substring(firstSlash + 1);
        }

        /**
         *
         * @return
         */
        @Override
        protected String getResultString() {

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < m_lines.length; i++) {
                if (i > 0) {
                    sb.append('\n');
                }

                sb.append("+ ").append(m_lines[i]);
            }

            return sb.toString();
        }
    }
}


