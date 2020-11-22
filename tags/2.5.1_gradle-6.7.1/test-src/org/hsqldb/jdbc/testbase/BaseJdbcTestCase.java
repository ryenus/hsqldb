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

package org.hsqldb.jdbc.testbase;

import java.math.BigDecimal;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.hsqldb.lib.Set;
import org.hsqldb.testbase.BaseTestCase;

/**
 * Abstract JDBC-focused Junit test case. <p>
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public abstract class BaseJdbcTestCase extends BaseTestCase {

    // We need a way of confirming compliance with
    // Tables B5 and B6 of JDBC 4.0 spec., outlining
    // the minimum conversions to be supported
    // by JDBC getXXX and setObject methods.

    private static final HashMap               jdbcGetXXXMap;
    private static final IntKeyHashMap         jdbcInverseGetXXXMap;
    private static final HashMap               jdbcSetObjectMap;
    private static final IntKeyHashMap         jdbcInverseSetObjectMap;
    private static final IntKeyIntValueHashMap dataTypeMap;

    private static final int[] tableB5AndB6ColumnDataTypes = new int[] {
        java.sql.Types.TINYINT, // ........................................... 0
        java.sql.Types.SMALLINT,
        java.sql.Types.INTEGER,
        java.sql.Types.BIGINT,
        java.sql.Types.REAL,
        java.sql.Types.FLOAT, // ............................................. 5
        java.sql.Types.DOUBLE,
        java.sql.Types.DECIMAL,
        java.sql.Types.NUMERIC,
        java.sql.Types.BIT,
        java.sql.Types.BOOLEAN, // .......................................... 10
        java.sql.Types.CHAR,
        java.sql.Types.VARCHAR,
        java.sql.Types.LONGVARCHAR,
        java.sql.Types.BINARY,
        java.sql.Types.VARBINARY, // ........................................ 15
        java.sql.Types.LONGVARBINARY,
        java.sql.Types.DATE,
        java.sql.Types.TIME,
        java.sql.Types.TIMESTAMP,
        java.sql.Types.ARRAY, // ............................................ 20
        java.sql.Types.BLOB,
        java.sql.Types.CLOB,
        java.sql.Types.STRUCT,
        java.sql.Types.REF,
        java.sql.Types.DATALINK, // ......................................... 25
        java.sql.Types.JAVA_OBJECT,
        java.sql.Types.ROWID,
        java.sql.Types.NCHAR,
        java.sql.Types.NVARCHAR,
        java.sql.Types.LONGNVARCHAR, // ..................................... 30
        java.sql.Types.NCLOB,
        java.sql.Types.SQLXML,
        java.sql.Types.OTHER
    };

    private static final String[] typeNames = new String[] {
        "TINYINT", // ........................................................ 0
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "REAL",
        "FLOAT", // .......................................................... 5
        "DOUBLE",
        "DECIMAL",
        "NUMERIC",
        "BIT",
        "BOOLEAN", // ....................................................... 10
        "CHAR",
        "VARCHAR",
        "LONGVARCHAR",
        "BINARY",
        "VARBINARY", // ..................................................... 15
        "LONGVARBINARY",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "CLOB",  // ......................................................... 20
        "BLOB",
        "ARRAY",
        "REF",
        "DATALINK",
        "STRUCT", // ........................................................ 25
        "JAVA_OBJECT",
        "ROWID",
        "NCHAR",
        "NVARCHAR",
        "LONGNVARCHAR", // .................................................. 30
        "NCLOB",
        "SQLXML",
        "OTHER"
    };

    private static final int typeCount = tableB5AndB6ColumnDataTypes.length;

    // JDBC 4.0, Table B6, Use of ResultSet getter Methods to Retrieve
    // JDBC Data Types

    // NOTE: Spec is missing for Types.OTHER
    //       We store Serializable, so we should support getXXX where XXX is
    //       serializable or the underlying data is a character or octet
    //       sequence (is inherently streamable)
    private static final String[][] requiredGetXXX = new String[][] {
        //  S
        //  M I
        //T A N
        //I L T..........................S
        //N L G..........................Q O
        //Y L E..........................L T
        //I I G..........................X H
        //N N E..........................M E
        //T T R..........................L R
        //                      0123456789012345678901234567890123
        {"getByte",            "1111111111111100000000000001000001"},
        {"getShort",           "1111111111111100000000000000000001"},
        {"getInt",             "1111111111111100000000000000000001"},
        {"getLong",            "1111111111111100000000000000000001"},
        {"getFloat",           "1111111111111100000000000000000001"},
        {"getDouble",          "1111111111111100000000000000000001"},
        {"getBigDecimal",      "1111111111111100000000000000000001"},
        {"getBoolean",         "1111111111111100000000000000000001"},
        {"getString",          "1111111111111111111100001000111001"},
        {"getNString",         "1111111111111111111100001000111001"},
        {"getBytes",           "0000000000000011100000000000000001"},
        {"getDate",            "0000000000011100010100000000000001"},
        {"getTime",            "0000000000011100001100000000000001"},
        {"getTimestamp",       "0000000000011100011100000000000001"},
        {"getAsciiStream",     "0000000000011111100010000000000101"},
        {"getBinaryStream",    "0000000000000011100001000000000011"},
        {"getCharacterStream", "0000000000011111100010000000111111"},
        {"getNCharacterStream","0000000000011111100010000000111111"},
        {"getClob",            "0000000000000000000010000000000101"},
        {"getNClob",           "0000000000000000000010000000000101"},
        {"getBlob",            "0000000000000000000001000000000001"},
        {"getArray",           "0000000000000000000000100000000000"},
        {"getRef",             "0000000000000000000000010000000000"},
        {"getURL",             "0000000000000000000000001000000000"},
        {"getObject",          "1111111111111111111111111111111111"},
        {"getRowId",           "0000000000000000000000000001000000"},
        {"getSQLXML",          "0000000000000000000000000000000010"}
    };

    // JDBC 4.0, Table B5, Conversions Performed by setObject Between
    // Java Object Types and Target JDBC Types

    // NOTE:     Spec is missing for Types.OTHER
    //           We store Serializable, so we should support setObject where
    //           object is serializable or the underlying data is a character
    //           or octet sequence (is inherently streamable)
    private static final Object[][] requiredSetObject = new Object[][] {
        //  S
        //  M I
        //T A N
        //I L T..........................S
        //N L G..........................Q O
        //Y L E..........................L T
        //I I G..........................X H
        //N N E..........................M E
        //T T R..........................L R
        //                          0123456789012345678901234567890123
        {String.class,             "1111111111111111111100000000111001"},
        {BigDecimal.class,         "1111111111111100000000000000000001"},
        {Boolean.class,            "1111111111111100000000000000000001"},
        {Byte.class,               "1111111111111100000000000000000001"},
        {Short.class,              "1111111111111100000000000000000001"},
        {Integer.class,            "1111111111111100000000000000000001"},
        {Long.class,               "1111111111111100000000000000000001"},
        {Float.class,              "1111111111111100000000000000000001"},
        {Double.class,             "1111111111111100000000000000000001"},
        {byte[].class,             "0000000000000011100000000000000001"},
        {java.sql.Date.class,      "0000000000011100010100000000000001"},
        {java.sql.Time.class,      "0000000000011100001000000000000001"},
        {java.sql.Timestamp.class, "0000000000011100011100000000000001"},
        {java.sql.Array.class,     "0000000000000000000010000000000000"},
        {java.sql.Blob.class,      "0000000000000000000001000000000001"},
        {java.sql.Clob.class,      "0000000000000000000000100000000001"},
        {java.sql.Struct.class,    "0000000000000000000000010000000000"},
        {java.sql.Ref.class,       "0000000000000000000000001000000000"},
        {java.net.URL.class,       "0000000000000000000000000100000000"},
        {Object.class,             "0000000000000000000000000010000001"},
        {java.sql.RowId.class,     "0000000000000000000000000001000000"},
        {java.sql.NClob.class,     "0000000000000000000000000000000101"},
        {java.sql.SQLXML.class,    "0000000000000000000000000000000010"}
    };

    static {
        //
        jdbcGetXXXMap           = new HashMap();
        jdbcInverseGetXXXMap    = new IntKeyHashMap();
        jdbcSetObjectMap        = new HashMap();
        jdbcInverseSetObjectMap = new IntKeyHashMap();
        dataTypeMap             = new IntKeyIntValueHashMap();

        for (int i = 0; i < typeCount; i++) {
            dataTypeMap.put(tableB5AndB6ColumnDataTypes[i], i);
        }

        for (int i = (requiredGetXXX.length - 1); i >= 0; i--) {

            Object   key         = requiredGetXXX[i][0];
            String   bits        = requiredGetXXX[i][1];
            String[] requiredGet = new String[typeCount];

            jdbcGetXXXMap.put(key, requiredGet);

            for (int j = (typeCount - 1); j >= 0; j--) {

                if (bits.charAt(j) == '1') {

                    requiredGet[j] = typeNames[j];

                    int dataType = tableB5AndB6ColumnDataTypes[j];
                    Set set      = (Set) jdbcInverseGetXXXMap.get(dataType);

                    if (set == null) {
                        set = new HashSet();

                        jdbcInverseGetXXXMap.put(dataType, set);
                    }

                    set.add(key);
                }
            }
        }

        for (int i = requiredSetObject.length - 1; i >= 0; i--) {

            Object   key         = requiredSetObject[i][0];
            String   bits        = (String) requiredSetObject[i][1];
            String[] requiredSet = new String[typeCount];

            jdbcSetObjectMap.put(key, requiredSet);

            for (int j = (typeCount - 1); j >= 0; j--) {
                if (bits.charAt(j) == '1') {

                    requiredSet[j] = typeNames[j];

                    int dataType = tableB5AndB6ColumnDataTypes[j];
                    Set set      = (Set) jdbcInverseSetObjectMap.get(dataType);

                    if (set == null) {
                        set = new HashSet();

                        jdbcInverseSetObjectMap.put(dataType, set);
                    }

                    set.add(key);
                }
            }
        }
    }

    /**
     * Retrieves whether a JDBC 4 compliant driver implementation is required
     * to support the given getter method for result columns with the given
     * underlying <tt>java.sql.Types</tt> SQL data type.
     *
     * @param methodName a jdbc getXXX method name.
     * @param dataType a java.sql.Types data type code
     * @return <tt>true</tt> if a JDBC 4 compliant driver implementation is required
     * to support the given getter method for result columns with the given
     * underlying <tt>java.sql.Types</tt> SQL data type, else <tt>false</tt>.
     */
    protected static boolean isRequiredGetXXX(String methodName, int dataType) {
        String[] requiredGet = (String[]) jdbcGetXXXMap.get(methodName);
        int      pos         = dataTypeMap.get(dataType, -1);

        return (pos >= 0) && (requiredGet != null) && (requiredGet[pos] != null);
    }

    /**
     * containing the names of the getter methods that a JDBC 4 compliant
     * driver implementation is required to support for result columns with
     * the given underlying <tt>java.sql.Types</tt> SQL data type.
     *
     * @param dataType a java.sql.Types data type code
     * @return the Set of names of the getter methods that a JDBC 4 compliant
     * driver implementation is required to support for result columns with the
     * given underlying <tt>java.sql.Types</tt>
     * SQL data type.
     */
    protected static Set getRequiredGetXXX(int dataType) {
        return (Set) jdbcInverseGetXXXMap.get(dataType);
    }

    /**
     * Retrieves whether a JDBC 4 compliant driver's PreparedStatement setObject
     * method is required to accept instances of the given class when the target
     * site has the given java.sql.Types SQL data type.
     *
     * @param clazz a candidate Class object
     * @param dataType a java.sql.Types data type code
     * @return true if a JDBC 4 compliant driver's PreparedStatement setObject
     * method is required to accept instances of the given class when the target
     * site has the given java.sql.Types SQL data type.
     */
    protected static boolean isRequiredSetObject(Class<?> clazz, int dataType) {
        String[] requiredSet = (String[]) jdbcSetObjectMap.get(clazz);
        int      pos         = dataTypeMap.get(dataType);

        return (pos >= 0) && (requiredSet != null) && (requiredSet[pos] != null);
    }

    /**
     * containing the fully qualified names of the classes whose instances a
     * JDBC 4 compliant driver's PreparedStatement setObject method is required
     * to accept when the target site has the given <tt>java.sql.Types</tt> SQL
     * data type.
     *
     * @param dataType for which to retrieve the set
     * @return corresponding to given data type
     */
    protected static Set getRequiredSetObject(int dataType) {
        return (Set) jdbcInverseSetObjectMap.get(dataType);
    }

    /**
     * Constructs a new JdbcTestCase.
     *
     * @param name test name
     */
    public BaseJdbcTestCase(String name) {
        super(name);
    }

    // for subclasses
    protected BaseJdbcTestCase() {
        super();
    }
}
