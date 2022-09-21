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
package org.hsqldb.testbase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Objects;

/**
 * Completely new approach, unrelated
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public abstract class BaseScriptedTestCase extends BaseTestCase {

    /**
     * For subclasses that dynamically generate their own name.
     */
    protected BaseScriptedTestCase() {
        super();
    }

    /**
     * Standard TestCase constructor, required for standard subclasses
     *
     * @param name of script resource.
     */
    public BaseScriptedTestCase(String name) {
        super(name);
    }

    protected Statement newStatement() throws Exception {
        return connectionFactory().createStatement(newConnection());
    }

    @Override
    @SuppressWarnings({"UseSpecificCatch", "BroadCatchBlock", "TooBroadCatch"})
    public void runTest() throws Throwable {
        final StringBuilder expected = getExpectedOutput(getName() + ".out");
        final StringBuilder actual = expected == null
                ? null
                : new StringBuilder(expected.length());
        try {
            this.executeTestScript(getName(), actual);
        } catch (Throwable t) {
            this.printException(t);
            throw t;
        }
        if (expected != null) {
            assertStringBuilderEquals(expected, actual);
        }
    }

    @SuppressWarnings("NestedAssignment")
    protected StringBuilder getExpectedOutput(final String resource) throws Exception {
        Objects.requireNonNull(resource, "resource must not be null");
        URL resourceURL = getClass().getResource(resource);
        if (resourceURL == null) {
            final File file = new File(resource);
            if (!file.exists()) {
                return null;
            }
            resourceURL = file.toURI().toURL();
        }
        if (resourceURL == null) {
            return null;
        }
        try (InputStream input = resourceURL.openStream();
                Reader reader = new InputStreamReader(input, StandardCharsets.UTF_8);) {
            @SuppressWarnings("StringBufferWithoutInitialCapacity")
            final StringBuilder sb = new StringBuilder();
            int ch;
            while (-1 != (ch = reader.read())) {
                sb.append((char) ch);
            }
            return sb;
        }
    }

    protected ScriptIterator getScriptIterator(final String resource) throws Exception {
        Objects.requireNonNull(resource, "resource must not be null");

        URL resourceURL = getClass().getResource(resource);

        if (resourceURL == null) {
            final File file = new File(resource);
            if (!file.exists()) {
                throw new IOException("No such resource: " + resource);
            }
            resourceURL = file.toURI().toURL();
        }

        return new ScriptIterator(resourceURL);
    }

    protected void executeTestScript(String resource) throws Exception {
        executeTestScript(resource, null);
    }

    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
    protected void executeTestScript(String resource, StringBuilder sb) throws Exception {
        final ScriptIterator itr = getScriptIterator(resource);
        int count = 0;

        printProgress("Opened test script: " + resource);

        try (Statement stmt = newStatement()) {
            printProgress("Script outut:");
            while (itr.hasNext()) {
                count++;
                final String sql = itr.next();
                String output;
                try {
                    boolean isResultSet = stmt.execute(sql);
                    int updateCount = stmt.getUpdateCount();
                    SQLWarning warnings = stmt.getWarnings();
                    while (warnings != null) {
                        output = warnings.toString();
                        if (sb != null) {
                            sb.append(output).append('\n');
                        }
                        println(output);
                        warnings = warnings.getNextWarning();
                    }
                    do {
                        if (isResultSet) {
                            final ResultSet rs = stmt.getResultSet();
                            output = resultSetToString(rs);
                            if (sb != null) {
                                sb.append(output).append('\n');
                            }
                            println(output);
                        } else {
                            output = "update count: " + updateCount;
                            if (sb != null) {
                                sb.append(output).append('\n');
                            }
                            println(output);
                        }
                        isResultSet = stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                        updateCount = stmt.getUpdateCount();
                    } while (updateCount != -1 && !isResultSet);
                } catch (Throwable t) {
                    output = t.toString();
                    if (sb != null) {
                        sb.append(output).append('\n');
                    }
                    println(output);
                }
            }
        }
    }

    protected String resultSetToString(final ResultSet rs) throws SQLException {
        final StringBuilder sb = new StringBuilder(256);
        final ResultSetMetaData rsmd = rs.getMetaData();
        final int columnCount = rsmd.getColumnCount();
        final Object[] values = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            values[i - 1] = rsmd.getColumnLabel(i);
            sb.append('%').append(rsmd.getColumnDisplaySize(i)).append("s");
        }
        final String format = sb.toString();
        sb.setLength(0);
        sb.append(String.format(format, values));
        if (rs.next()) {
            do {
                sb.append('\n');
                for (int i = 1; i <= columnCount; i++) {
                    values[i - 1] = rs.getString(i);
                }
                sb.append(String.format(format, values));
            } while (rs.next());
        }
        return sb.toString();
    }
}
