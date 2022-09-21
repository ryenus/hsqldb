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
 * Enumerates the known result set type values.
 *
 * Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 *
 * @version 2.6.x
 * @since 2.6.x
 */
public enum ResultSetType {
    /**
     * See {@link ResultSet#TYPE_FORWARD_ONLY}.
     */
    ForwardOnly(ResultSet.TYPE_FORWARD_ONLY),
    /**
     * See {@link ResultSet#TYPE_SCROLL_INSENSITIVE}.
     */
    ScrollInsensitive(ResultSet.TYPE_SCROLL_INSENSITIVE),
    /**
     * See {@link ResultSet#TYPE_SCROLL_SENSITIVE}.
     */
    ScrollSensitive(ResultSet.TYPE_SCROLL_SENSITIVE),
    /**
     * Denotes a value that is not defined.
     */
    Undefined(-1);

    public static final Comparator<ResultSetType> COMPARATOR
            = new Comparator<ResultSetType>() {
        @Override
        public int compare(final ResultSetType type1,
                final ResultSetType type2) {

            int value1 = type1 == null
                    ? ResultSetType.getDefault().value
                    : type1.value;
            int value2 = type2 == null
                    ? ResultSetType.getDefault().value
                    : type2.value;

            return value1 - value2;
        }
    };
    
    /**
     * as a new, modifiable EnmumSet.
     * 
     * @return a new, modifiable set.
     */    
    public static EnumSet<ResultSetType> definedValues() {
        EnumSet<ResultSetType> set = EnumSet.allOf(ResultSetType.class);
        
        set.remove(Undefined);
        
        return set;
    }

    /**
     * that is given.
     *
     * @param value for which to produce a corresponding instance.
     * @return a corresponding instance, or {@link #Undefined} to
     * indicate no
     * such instance.
     */
    public static ResultSetType forValue(final int value) {
        for (ResultSetType rst : ResultSetType.values()) {
            if (value == rst.value) {
                return rst;
            }
        }

        return Undefined;
    }

    /**
     * which, by definition, is {@link #ForwardOnly}.
     *
     * @return {@link #ForwardOnly}.
     */
    public static ResultSetType getDefault() {
        return ForwardOnly;
    }

    private final int value;

    ResultSetType(final int value) {
        this.value = value;
    }

    /**
     * as it is declared in {@link ResultSet}.
     *
     * @return the declared field name.
     */
    public String fieldName() {
        switch (this) {
            case ForwardOnly: {
                return "TYPE_FORWARD_ONLY";
            }
            case ScrollInsensitive: {
                return "TYPE_SCROLL_INSENSITIVE";
            }
            case ScrollSensitive: {
                return "TYPE_SCROLL_SENSITIVE";
            }
            default: {
                return "undefined";
            }
        }
    }

    /**
     * value.
     *
     * @return true if so, else false.
     */
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
     * versus forward-only.
     *
     *
     * @return true if so, else false.
     */
    public boolean isScrollable() {
        switch (this) {
            case ScrollInsensitive:
            case ScrollSensitive: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    /**
     * to changes to the data that underlies a {@link ResultSet}.
     *
     * @return true if so, else false.
     */
    public boolean isSensitive() {
        return this.value == ScrollSensitive.value;
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
        return isDefined() && conn.getMetaData().supportsResultSetType(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("java.sql.ResultSet.%s <%s>", this.fieldName(),
                this.value());
    }

    /**
     * denoted by this instance.
     *
     * @return the denoted integer value; -1 if undefined.
     */
    public int value() {
        return value;
    }
    
}
