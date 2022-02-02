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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.EnumSet;

/**
 * Enumerates the known result set concurrency values.
 *
 * Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 *
 * @version 2.6.x
 * @since 2.6.x
 */
public enum ResultSetConcurrency {
    /**
     * See {@link ResultSet#CONCUR_READ_ONLY}.
     */
    ReadOnly(ResultSet.CONCUR_READ_ONLY),
    /**
     * See {@link ResultSet#CONCUR_UPDATABLE}.
     */
    Updatable(ResultSet.CONCUR_UPDATABLE),
    /**
     * Denotes a value that is not known to be defined.
     */
    Undefined(-1);

    /**
     * Compares values using upgraded / downgraded feature semantics.
     * <p>
     * So, {@link #Updatable} {@code >} {@link #ReadOnly} {@code >}
     * {@link #Undefined}, where {@code null} is interpreted as
     * {@link #getDefault()}.
     */
    public static final Comparator<ResultSetConcurrency> COMPARATOR
            = new Comparator<ResultSetConcurrency>() {
        @Override
        public int compare(final ResultSetConcurrency e1,
                final ResultSetConcurrency e2) {

            int value1 = e1 == null
                    ? ResultSetConcurrency.getDefault().value
                    : e1.value;
            int value2 = e2 == null
                    ? ResultSetConcurrency.getDefault().value
                    : e2.value;

            return value1 - value2;
        }
    };

    /**
     * as a new, modifiable EnmumSet.
     *
     * @return a new, modifiable set.
     */
    public static EnumSet<ResultSetConcurrency> definedValues() {
        EnumSet<ResultSetConcurrency> set = EnumSet.allOf(ResultSetConcurrency.class);

        set.remove(Undefined);

        return set;
    }

    /**
     * that is given.
     *
     * @param value for which to produce a corresponding instance.
     * @return a corresponding instance, or {@link #Undefined} to indicate no
     * such instance.
     */
    public static ResultSetConcurrency forValue(final int value) {
        for (ResultSetConcurrency rsc : ResultSetConcurrency.values()) {
            if (value == rsc.value) {
                return rsc;
            }
        }

        return Undefined;
    }

    /**
     * which, by definition, is {@link #ReadOnly}.
     *
     * @return {@link #ReadOnly}
     */
    public static ResultSetConcurrency getDefault() {
        return ReadOnly;
    }

    private final int value;

    ResultSetConcurrency(final int value) {
        this.value = value;
    }

    public String fieldName() {
        switch (this) {
            case ReadOnly: {
                return "CONCUR_READ_ONLY";
            }
            case Updatable: {
                return "CONCUR_UPDATABLE";
            }
            default: {
                return "undefined";
            }
        }
    }

    public boolean isDefault() {
        return this.value == getDefault().value;
    }

    /**
     * value.
     *
     * @return true if defined, else false.
     */
    public boolean isDefined() {
        return this.value != Undefined.value;
    }

    /**
     * for the given connection.
     *
     * @param conn to test.
     * @param type to test.
     * @return true if so, else false.
     *
     * @throws SQLException if a database access error occurs.
     */
    public boolean isSupported(final Connection conn, ResultSetType type) throws SQLException {
        return type != null
                && type.isDefined()
                && this.isDefined()
                && conn.getMetaData().supportsResultSetConcurrency(
                        type.value(), this.value);
    }

    /**
     * value.
     *
     * @return true if so, else false.
     */
    public boolean isUpdatable() {
        return this == Updatable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("java.sql.ResultSet.%s <%s>", this.fieldName(), this.value());
    }

    /**
     * denoted by this instance.
     *
     * @return the denoted integer value; -1 if undefined.
     */
    public int value() {
        return this.value;
    }

}
