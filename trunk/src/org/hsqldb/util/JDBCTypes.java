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


package org.hsqldb.util;

import java.util.Hashtable;

/**
 * Base class for conversion from a different databases
 *
 * @author Nicolas BAZIN
 * @version 1.7.0
 */
class JDBCTypes {

    public static final int JAVA_OBJECT = 2000;
    public static final int DISTINCT    = 2001;
    public static final int STRUCT      = 2002;
    public static final int ARRAY       = 2003;
    public static final int BLOB        = 2004;
    public static final int CLOB        = 2005;
    public static final int REF         = 2006;
    private Hashtable       hStringJDBCtypes;
    private Hashtable       hIntJDBCtypes;

    JDBCTypes() {

        hStringJDBCtypes = new Hashtable();
        hIntJDBCtypes    = new Hashtable();

        hStringJDBCtypes.put(Integer.valueOf(ARRAY), "ARRAY");
        hStringJDBCtypes.put(Integer.valueOf(BLOB), "BLOB");
        hStringJDBCtypes.put(Integer.valueOf(CLOB), "CLOB");
        hStringJDBCtypes.put(Integer.valueOf(DISTINCT), "DISTINCT");
        hStringJDBCtypes.put(Integer.valueOf(JAVA_OBJECT), "JAVA_OBJECT");
        hStringJDBCtypes.put(Integer.valueOf(REF), "REF");
        hStringJDBCtypes.put(Integer.valueOf(STRUCT), "STRUCT");

        //
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.BIGINT), "BIGINT");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.BINARY), "BINARY");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.BIT), "BIT");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.CHAR), "CHAR");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.DATE), "DATE");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.DECIMAL), "DECIMAL");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.DOUBLE), "DOUBLE");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.FLOAT), "FLOAT");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.INTEGER), "INTEGER");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.LONGVARBINARY),
                             "LONGVARBINARY");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.LONGVARCHAR),
                             "LONGVARCHAR");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.NULL), "NULL");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.NUMERIC), "NUMERIC");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.OTHER), "OTHER");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.REAL), "REAL");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.SMALLINT),
                             "SMALLINT");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.TIME), "TIME");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.TIMESTAMP),
                             "TIMESTAMP");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.TINYINT), "TINYINT");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.VARBINARY),
                             "VARBINARY");
        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.VARCHAR), "VARCHAR");

        hStringJDBCtypes.put(Integer.valueOf(java.sql.Types.BOOLEAN), "BOOLEAN");
        //
        hIntJDBCtypes.put("ARRAY", Integer.valueOf(ARRAY));
        hIntJDBCtypes.put("BLOB", Integer.valueOf(BLOB));
        hIntJDBCtypes.put("CLOB", Integer.valueOf(CLOB));
        hIntJDBCtypes.put("DISTINCT", Integer.valueOf(DISTINCT));
        hIntJDBCtypes.put("JAVA_OBJECT", Integer.valueOf(JAVA_OBJECT));
        hIntJDBCtypes.put("REF", Integer.valueOf(REF));
        hIntJDBCtypes.put("STRUCT", Integer.valueOf(STRUCT));

        //
        hIntJDBCtypes.put("BIGINT", Integer.valueOf(java.sql.Types.BIGINT));
        hIntJDBCtypes.put("BINARY", Integer.valueOf(java.sql.Types.BINARY));
        hIntJDBCtypes.put("BIT", Integer.valueOf(java.sql.Types.BIT));
        hIntJDBCtypes.put("CHAR", Integer.valueOf(java.sql.Types.CHAR));
        hIntJDBCtypes.put("DATE", Integer.valueOf(java.sql.Types.DATE));
        hIntJDBCtypes.put("DECIMAL", Integer.valueOf(java.sql.Types.DECIMAL));
        hIntJDBCtypes.put("DOUBLE", Integer.valueOf(java.sql.Types.DOUBLE));
        hIntJDBCtypes.put("FLOAT", Integer.valueOf(java.sql.Types.FLOAT));
        hIntJDBCtypes.put("INTEGER", Integer.valueOf(java.sql.Types.INTEGER));
        hIntJDBCtypes.put("LONGVARBINARY",
                          Integer.valueOf(java.sql.Types.LONGVARBINARY));
        hIntJDBCtypes.put("LONGVARCHAR",
                          Integer.valueOf(java.sql.Types.LONGVARCHAR));
        hIntJDBCtypes.put("NULL", Integer.valueOf(java.sql.Types.NULL));
        hIntJDBCtypes.put("NUMERIC", Integer.valueOf(java.sql.Types.NUMERIC));
        hIntJDBCtypes.put("OTHER", Integer.valueOf(java.sql.Types.OTHER));
        hIntJDBCtypes.put("REAL", Integer.valueOf(java.sql.Types.REAL));
        hIntJDBCtypes.put("SMALLINT", Integer.valueOf(java.sql.Types.SMALLINT));
        hIntJDBCtypes.put("TIME", Integer.valueOf(java.sql.Types.TIME));
        hIntJDBCtypes.put("TIMESTAMP", Integer.valueOf(java.sql.Types.TIMESTAMP));
        hIntJDBCtypes.put("TINYINT", Integer.valueOf(java.sql.Types.TINYINT));
        hIntJDBCtypes.put("VARBINARY", Integer.valueOf(java.sql.Types.VARBINARY));
        hIntJDBCtypes.put("VARCHAR", Integer.valueOf(java.sql.Types.VARCHAR));
        hIntJDBCtypes.put("BOOLEAN", Integer.valueOf(java.sql.Types.BOOLEAN));
    }

    public Hashtable getHashtable() {
        return hStringJDBCtypes;
    }

    public String toString(int type) {
        return (String) hStringJDBCtypes.get(Integer.valueOf(type));
    }

    public int toInt(String type) {

        Integer tempInteger = (Integer) hIntJDBCtypes.get(type);

        return tempInteger.intValue();
    }
}
