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


package org.hsqldb.types;

import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCBlobClient;
import org.hsqldb.lib.StringConverter;

/**
 * Type object for BLOB.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public final class BlobType extends BinaryType {

    public static final long maxBlobPrecision = 1024L * 1024 * 1024 * 1024;
    public static final int  defaultBlobSize  = 1024 * 1024 * 16;

    public BlobType(long precision) {
        super(Types.SQL_BLOB, precision);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {
        return Types.BLOB;
    }

    public Class getJDBCClass() {
        return java.sql.Blob.class;
    }

    public String getJDBCClassName() {
        return "java.sql.Blob";
    }

    public String getNameString() {
        return Tokens.T_BLOB;
    }

    public String getFullNameString() {
        return "BINARY LARGE OBJECT";
    }

    public String getDefinition() {

        long   factor     = precision;
        String multiplier = null;

        if (precision % (1024 * 1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024 * 1024);
            multiplier = Tokens.T_G_FACTOR;
        } else if (precision % (1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024);
            multiplier = Tokens.T_M_FACTOR;
        } else if (precision % (1024) == 0) {
            factor     = precision / (1024);
            multiplier = Tokens.T_K_FACTOR;
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(getNameString());
        sb.append('(');
        sb.append(factor);

        if (multiplier != null) {
            sb.append(multiplier);
        }

        sb.append(')');

        return sb.toString();
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean requiresPrecision() {
        return false;
    }

    public long getMaxPrecision() {
        return maxBlobPrecision;
    }

    public boolean isBinaryType() {
        return true;
    }

    public boolean isLobType() {
        return true;
    }

    public int compare(Session session, Object a, Object b) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        if (b instanceof BinaryData) {
            return session.database.lobManager.compare((BlobData) a,
                    ((BlobData) b).getBytes());
        }

        return session.database.lobManager.compare((BlobData) a, (BlobData) b);
    }

    /** @todo - implement */
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return a;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return null;
        }

        if (otherType.typeCode == Types.SQL_BLOB) {
            return a;
        }

        if (otherType.typeCode == Types.SQL_BINARY
                || otherType.typeCode == Types.SQL_VARBINARY) {
            BlobData b    = (BlobData) a;
            BlobData blob = session.createBlob(b.length(session));

            blob.setBytes(session, 0, b.getBytes());

            return blob;
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        // conversion to Blob via PreparedStatement.setObject();
        if (a instanceof byte[]) {
            return new BinaryData((byte[]) a, false);
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        byte[] bytes = ((BlobData) a).getBytes();

        return StringConverter.byteArrayToHexString(bytes);
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return Tokens.T_NULL;
        }

        byte[] bytes = ((BlobData) a).getBytes();

        return StringConverter.byteArrayToSQLHexString(bytes);
    }

    public Object convertJavaToSQL(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof JDBCBlobClient) {
            return ((JDBCBlobClient) a).getBlob();
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof BlobDataID) {
            BlobDataID blob = (BlobDataID) a;

            return new JDBCBlobClient(session, blob);
        }

        throw Error.error(ErrorCode.X_42561);
    }
}
