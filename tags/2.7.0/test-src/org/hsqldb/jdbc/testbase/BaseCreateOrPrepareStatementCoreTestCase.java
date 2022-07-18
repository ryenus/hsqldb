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

package org.hsqldb.jdbc.testbase;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import static junit.framework.TestCase.assertEquals;
import org.hsqldb.error.ErrorCode;

/**
 * Provides the basis for testing the core methods of {@link Connection} that
 * create statements.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public abstract class BaseCreateOrPrepareStatementCoreTestCase
        extends BaseJdbcTestCase {

    /**
     * for the given arguments.
     *
     * @param type value to test
     * @param concurrency value to test
     * @param holdability value to test
     * @param namePrefix indicating the target method.
     * @return a new String
     */
    protected static String computeTestName(
            final ResultSetType type,
            final ResultSetConcurrency concurrency,
            final ResultSetHoldability holdability,
            final String namePrefix) {

        return String.format("%s_%s_%s", namePrefix, type, concurrency,
                holdability);
    }

    /**
     * to test.
     */
    protected final ResultSetConcurrency m_concurrency;

    /**
     * to test.
     */
    protected final ResultSetHoldability m_holdability;

    /**
     * to test.
     */
    protected final ResultSetType m_type;

    /**
     * Constructs a new instance from the given arguments.
     *
     * @param type to test.
     * @param concurrency to test.
     * @param holdability to test.
     * @param namePrefix indicating the target method.
     */
    protected BaseCreateOrPrepareStatementCoreTestCase(
            final ResultSetType type, final ResultSetConcurrency concurrency,
            final ResultSetHoldability holdability, final String namePrefix) {
        super(computeTestName(type, concurrency, holdability,
                namePrefix));
        this.m_type = type;
        this.m_concurrency = concurrency;
        this.m_holdability = holdability;
        
        if (!type.isDefined()) {
            throw new RuntimeException();
        }
        
//        assert type.isDefined();
//        assert concurrency.isDefined();
//        assert holdability.isDefined();
    }

    /**
     * if any, issued on the given connection {@code conn}, as the result of
     * creating the given statement, {@code stmt}.
     *
     * Test fails if requested type was defined and downgraded without warning or
     * requested concurrency was defined and downgraded without warning or
     * holdability was defined and changed without warning.
     *
     * @param conn to check.
     * @param stmt to check.
     *
     * @throws SQLException if a database access error occurs.
     */
    protected void checkWarnings(final Connection conn, final Statement stmt) throws SQLException {
        final SQLWarning warnings = conn.getWarnings();
        final ResultSetType requestedType = this.getResultSetType();
        final ResultSetType actualType = ResultSetType.forValue(stmt.getResultSetType());
        final ResultSetConcurrency requestedConcurrency = this.getResultSetConcurrency();
        final ResultSetConcurrency actualConcurrency = ResultSetConcurrency.forValue(stmt.getResultSetConcurrency());
        final ResultSetHoldability requestedHoldability = this.getResultSetHoldability();
        final ResultSetHoldability actualHoldability = ResultSetHoldability.forValue(stmt.getResultSetHoldability());

        final boolean typeDowngraded = requestedType.isDefined() && ResultSetType.COMPARATOR.compare(requestedType, actualType) > 0;
        final boolean concurrenyDowngraded = requestedConcurrency.isDefined() && ResultSetConcurrency.COMPARATOR.compare(requestedConcurrency, actualConcurrency) > 0;
        final boolean holdabilityChanged = requestedHoldability.isDefined() && ResultSetHoldability.COMPARATOR.compare(requestedHoldability, actualHoldability) != 0;

        if (typeDowngraded && !this.hasSensitivityWarning(warnings)) {
            assertEquals("Requested ResultSet Type was downgraded without SQL Warning(s)", requestedType, actualType);
        }
        if (concurrenyDowngraded && !this.hasUpdatabilityWarning(warnings)) {
            assertEquals("Requested ResultSet Concurrency was downgraded without SQL Warning(s)", requestedConcurrency, actualConcurrency);
        }
        if (holdabilityChanged && !this.hasHoldabilityWarning(warnings)) {
            assertEquals("ResultSet Holdability was changed without SQL Warning(s)", requestedHoldability, actualHoldability);
        }

    }

    /**
     * to test.
     * 
     * @return the value.
     */
    protected ResultSetConcurrency getResultSetConcurrency() {
        return m_concurrency;
    }

    /**
     * to test.
     * 
     * @return the value.
     */
    protected ResultSetHoldability getResultSetHoldability() {
        return m_holdability;
    }

    /**
     * to test.
     * 
     * @return the value.
     */
    protected ResultSetType getResultSetType() {
        return m_type;
    }

    /**
     * to test.
     * 
     * @return the value.
     * 
     * @throws UnsupportedOperationException if not overridden.
     */
    protected String getSql() {
        throw new UnsupportedOperationException();
    }

    private boolean hasHoldabilityWarning(SQLWarning warnings) {
        while (warnings != null) {
            if (isHoldabilityWarning(warnings)) {
                return true;
            }
            warnings = warnings.getNextWarning();
        }
        return false;
    }

    private boolean hasSensitivityWarning(SQLWarning warnings) {
        while (warnings != null) {
            if (isSensitivityWarning(warnings)) {
                return true;
            }
            warnings = warnings.getNextWarning();
        }
        return false;
    }

    private boolean hasUpdatabilityWarning(SQLWarning warnings) {
        while (warnings != null) {
            if (isSensitivityWarning(warnings)) {
                return true;
            }
            warnings = warnings.getNextWarning();
        }
        return false;
    }

    /**
     * as per some vendor-specific determination.
     * <p>
     * By default, performs an HSQLDB-specific test, but can be overridden.
     * @param warning to test
     * @return true if so, else false.
     */
    protected boolean isHoldabilityWarning(final SQLWarning warning) {
        return warning.getErrorCode() == ErrorCode.W_36503;
    }

    /**
     * as per some vendor-specific determination.
     * <p>
     * By default, performs an HSQLDB-specific test, but can be overridden.
     * @param warning to test
     * @return true if so, else false.
     */
    protected boolean isSensitivityWarning(final SQLWarning warning) {
        return warning.getErrorCode() == ErrorCode.W_36501;
    }

    /**
     * as per some vendor-specific determination.
     * <p>
     * By default, performs an HSQLDB-specific test, but can be overridden.
     * @param warning to test
     * @return true if so, else false.
     */
    protected boolean isUpdatabilityWarning(final SQLWarning warning) {
        return warning.getErrorCode() == ErrorCode.W_36502;
    }

    /**
     * for the given parameters.
     * <p>
     * It is the responsibility of the subclass to supply {@link #getSql()}
     * and any other parameters under test.
     * 
     * @param conn used to create the statement.
     * @param type to specify.
     * @param concurrency to specify.
     * @param holdability to specify.
     * @return a new instance.
     * @throws SQLException if a database access error occurs.
     */
    protected abstract Statement materializeStatement(final Connection conn,
            final ResultSetType type, final ResultSetConcurrency concurrency,
            final ResultSetHoldability holdability) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runTest() throws Throwable {
        final ResultSetType type = getResultSetType();
        final ResultSetConcurrency concurrency = getResultSetConcurrency();
        final ResultSetHoldability holdability = getResultSetHoldability();
        final Connection conn = newConnection();
        final Statement stmt = materializeStatement(conn, type, concurrency, holdability);
        connectionFactory().registerStatement(stmt);
        checkWarnings(conn, stmt);
    }

}
