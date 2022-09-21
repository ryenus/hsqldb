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
package org.hsqldb.jdbc.testbase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.EnumSet;

/**
 * Enumerates the known result set holdability values.
 *
 * Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 *
 * @version 2.6.x
 * @since 2.6.x
 */
public enum ResultSetHoldability implements Comparable<ResultSetHoldability> {
    /**
     * See {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}.
     */
    CloseCursorsAtCommit(ResultSet.CLOSE_CURSORS_AT_COMMIT),
    /**
     * See {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}.
     */
    HoldCursorsOverCommit(ResultSet.HOLD_CURSORS_OVER_COMMIT),
    /**
     * Denotes a value that is not defined.
     */
    Undefined(-1);

    /**
     * Compares values using upgraded / downgraded feature semantics.
     * <p>
     * holdability has no documented default, so null, {@link #Undefined}, and
     * {@link #getDefault()} are considered to be identical and "less than"
     * {@link #CloseCursorsAtCommit} which in turn is deemed to be "less than"
     * {@link #HoldCursorsOverCommit}.
     */
    public static final Comparator<ResultSetHoldability> COMPARATOR
            = new Comparator<ResultSetHoldability>() {
        /**
         * {@inheritDoc}
         */
        @Override
        public int compare(final ResultSetHoldability e1,
                final ResultSetHoldability e2) {

            int value1 = e1 == null
                    ? ResultSetHoldability.getDefault().value
                    : e1.value;
            int value2 = e2 == null
                    ? ResultSetHoldability.getDefault().value
                    : e2.value;

            //*** NOTE ***//
            // order ostensibly reversed, because  ResultSet.CLOSE_CURSORS_AT_COMMIT (2)
            // is numerically > ResultSet.HOLD_CURSORS_OVER_COMMIT (1), but is a
            // "downgrade" from holdable to not holdable
            return value2 - value1;
        }

    };
    
    /**
     * as a new, modifiable EnmumSet.
     * 
     * @return a new, modifiable set.
     */
    public static EnumSet<ResultSetHoldability> definedValues() {
        EnumSet<ResultSetHoldability> set = EnumSet.allOf(ResultSetHoldability.class);
        
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
    public static ResultSetHoldability forValue(final int value) {
        for (ResultSetHoldability rsh : ResultSetHoldability.values()) {
            if (value == rsh.value) {
                return rsh;
            }
        }
        return Undefined;
    }

    /**
     * which, by definition, is {@link #Undefined}.
     *
     * @return {@link #Undefined}
     */
    public static ResultSetHoldability getDefault() {
        return Undefined;
    }

    private final int value;

    ResultSetHoldability(final int value) {
        this.value = value;
    }

    /**
     * as it is declared in {@link ResultSet}.
     *
     * @return the declared field name.
     */
    public String fieldName() {
        switch (this) {
            case HoldCursorsOverCommit: {
                return "HOLD_CURSORS_OVER_COMMIT";
            }
            case CloseCursorsAtCommit: {
                return "CLOSE_CURSORS_AT_COMMIT";
            }
            default: {
                return "undefined";
            }
        }
    }

    /**
     * never. Holdability has no defined default.
     *
     * @return false.
     */
    public boolean isDefault() {
        return false;
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
     * over commit.
     *
     * @return true if so, else false.
     */
    public boolean isHoldable() {
        return this.value == HoldCursorsOverCommit.value;
    }

    /**
     * for the given connection.
     *
     * @param conn to test.
     * @return true if so, else false.
     *
     * @throws SQLException if a database access error occurs.
     */
    public boolean isSupported(final Connection conn) throws SQLException {
        return isDefined()
                && conn.getMetaData().supportsResultSetHoldability(value);
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
