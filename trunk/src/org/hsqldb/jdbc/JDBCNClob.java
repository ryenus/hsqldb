/* Copyright (c) 2001-2024, The HSQL Development Group
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

import java.sql.NClob;

/**
 * The mapping in the Java&trade; programming language
 * for the SQL {@code NCLOB} type.
 * An SQL {@code NCLOB} is a built-in type
 * that stores a Character Large Object using the National Character Set
 *  as a column value in a row of  a database table.
 * <P>The {@code NClob} interface extends the {@code Clob} interface
 * which provides methods for getting the
 * length of an SQL {@code NCLOB} value,
 * for materializing a {@code NCLOB} value on the client, and for
 * searching for a substring or {@code NCLOB} object within a
 * {@code NCLOB} value. A {@code NClob} object, just like a {@code Clob} object, is valid for the duration
 * of the transaction in which it was created.
 * Methods in the interfaces {@link java.sql.ResultSet},
 * {@link java.sql.CallableStatement}, and {@link java.sql.PreparedStatement}, such as
 * {@code getNClob} and {@code setNClob} allow a programmer to
 * access an SQL {@code NCLOB} value.  In addition, this interface
 * has methods for updating a {@code NCLOB} value.
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <p class="rshead">HSQLDB-Specific Information:</p>
 *
 * First, it should be noted that since HSQLDB represents all character data
 * internally as Java UNICODE (UTF16) String objects, there is not currently any
 * appreciable difference between the HSQLDB XXXCHAR types and the SQL 2003
 * NXXXCHAR and NCLOB types. <p>
 *
 * See {@link org.hsqldb.jdbc.JDBCClob} for further information.
 *
 * </div>
 * <!-- end Release-specific documentation -->
 *
 * @since JDK 1.6, HSQLDB 2.7.3
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @see JDBCClob
 * @see JDBCClobClient
 */
public class JDBCNClob extends JDBCClob implements NClob {

    protected JDBCNClob() {
        super();
    }

    public JDBCNClob(String data) throws java.sql.SQLException {
        super(data);
    }
}
