/* Copyright (c) 2001-2007, The HSQL Development Group
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

//#ifdef JDBC4
import java.sql.NClob;

//#endif JDBC4

/**
 * The mapping in the Java<sup><font size=-2>TM</font></sup> programming language
 * for the SQL <code>NCLOB</code> type.
 * An SQL <code>NCLOB</code> is a built-in type
 * that stores a Character Large Object using the National Character Set
 *  as a column value in a row of  a database table.
 * <P>The <code>NClob</code> interface extends the <code>Clob</code> interface
 * which provides provides methods for getting the
 * length of an SQL <code>NCLOB</code> value,
 * for materializing a <code>NCLOB</code> value on the client, and for
 * searching for a substring or <code>NCLOB</code> object within a
 * <code>NCLOB</code> value. A <code>NClob</code> object, just like a <code>Clob</code> object, is valid for the duration
 * of the transaction in which it was created.
 * Methods in the interfaces {@link java.sql.ResultSet},
 * {@link java.sql.CallableStatement}, and {@link java.sql.PreparedStatement}, such as
 * <code>getNClob</code> and <code>setNClob</code> allow a programmer to
 * access an SQL <code>NCLOB</code> value.  In addition, this interface
 * has methods for updating a <code>NCLOB</code> value.
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * First, it should be noted that since HSQLDB represents all character data
 * internally as Java UNICODE (UTF16) String objects, there is not currently any
 * appreciable difference between the HSQLDB XXXCHAR types and the SQL 2003
 * NXXXCHAR and NCLOB types. <p>
 *
 * Including 1.8.x, the HSQLDB driver does not implement NClob using an SQL
 * locator(NCLOB).  That is, an HSQLDB NClob object does not contain a logical
 * pointer to SQL NCLOB data; rather it directly contains a representation of
 * the data (a java String). As a result, an HSQLDB NClob object is itself
 * valid beyond the duration of the transaction in which is was created,
 * although it does not necessarily represent a corresponding value
 * on the database. <p>
 *
 * All interface methods for updating an NCLOB value are now supported for local
 * use when the product is built under JDK 1.6+ and the NClob instance is
 * constructed as a result of calling jdbcConnection.createBlob(). <p>
 *
 * See {@link org.hsqldb.jdbc.jdbcClob} for further information concerning
 * current Clob update restrictions. <p>
 *
 * </div>
 * <!-- end Release-specific documentation -->
 *
 * @since JDK 1.6, HSQLDB 1.8.x
 * @author boucherb@users
 * @see jdbcClob
 */
public class jdbcNClob extends jdbcClob implements NClob {

    public jdbcNClob(String data) throws java.sql.SQLException {
        super(data);
    }

    protected jdbcNClob() {
        super();
    }
}
