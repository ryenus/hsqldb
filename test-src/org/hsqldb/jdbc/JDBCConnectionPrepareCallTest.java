/* Copyright (c) 2001-2021, The HSQL Development Group
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
package org.hsqldb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseCreateOrPrepareStatementCoreTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.jdbc.testbase.ResultSetConcurrency;
import org.hsqldb.jdbc.testbase.ResultSetHoldability;
import org.hsqldb.jdbc.testbase.ResultSetType;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(JDBCConnection.class)
public class JDBCConnectionPrepareCallTest extends BaseCreateOrPrepareStatementCoreTestCase {

    public static Test suite() {
        final TestSuite suite = new TestSuite(
                JDBCConnectionPrepareCallTest.class.getSimpleName());

        for (ResultSetType type : ResultSetType.definedValues()) {
            for (ResultSetConcurrency concurrency 
                    : ResultSetConcurrency.definedValues()) {
                for (ResultSetHoldability holdability 
                        : ResultSetHoldability.values()) {
                    suite.addTest(new JDBCConnectionPrepareCallTest(
                            type, concurrency, holdability));
                }
            }
        }

        return suite;
    }

    public static void main(final String[] args) {

        junit.textui.TestRunner.run(suite());
    }

    public JDBCConnectionPrepareCallTest(
            ResultSetType type,
            ResultSetConcurrency concurrency,
            ResultSetHoldability holdability) {
        super(type, concurrency, holdability, "prepareCall");

    }

    @Override
    protected String getSql() {
        return "call 1;";
    }

    @Override
    protected Statement materializeStatement(final Connection conn,
            final ResultSetType type, final ResultSetConcurrency concurrency,
            final ResultSetHoldability holdability) throws SQLException {
        final String sql = getSql();
        Statement stmt;
        if (type.isDefined() && concurrency.isDefined()) {
            if (holdability.isDefined()) {
                stmt = conn.prepareCall(sql, type.value(),
                        concurrency.value(), holdability.value());
            } else {
                stmt = conn.prepareCall(sql, type.value(),
                        concurrency.value());
            }
        } else {
            stmt = conn.prepareCall(sql);
        }
        return stmt;
    }
}
