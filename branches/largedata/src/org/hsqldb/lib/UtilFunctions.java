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


package org.hsqldb.lib;

import java.sql.Blob;
import java.sql.Connection;

import org.hsqldb.Session;
import org.hsqldb.jdbc.JDBCBlobClient;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.persist.LobStore;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.Database;
import org.hsqldb.Table;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.SchemaManager;
import org.hsqldb.HsqlNameManager.HsqlName;

public class UtilFunctions {

    /**
     * Returns the length of a blob. If blob is missing due to bug, returns -1
     */
    public static Long getBlobLength(Connection conn, Blob lob) {

        if (lob == null) {
            return null;
        }

        try {
            Session session = (Session) ((JDBCConnection) conn).getSession();
            BlobDataID blob = ((JDBCBlobClient) lob).getBlob();

            return blob.length(session);
        } catch (Exception e) {
            return -1L;
        }
    }

    /**
     * Returns a byte array for a deleted blob or clob block. The blockId and
     * count arguments must be values from a row of the
     * SYSTEM_LOBS.BLOCK table for columns BLOCK_ADDR and BLOCK_COUNT
     *
     * @param blockId BLOCK_ADDR value
     * @param count BLOCK_COUNT value
     */
    public static byte[] getDeletedBlob(Connection conn, int blockId,
                                        int count) {

        Session  session = (Session) ((JDBCConnection) conn).getSession();
        LobStore store   = session.database.lobManager.getLobStore();

        return store.getBlockBytes(blockId, count);
    }

    public static long getRowCount(Connection conn, String schema,
                                  String tableName) {

        Session  session  = (Session) ((JDBCConnection) conn).getSession();
        Database database = session.getDatabase();
        Table table = database.schemaManager.getTable(session, tableName,
            schema);
        PersistentStore store = table.getRowStore(session);

        return store.elementCount(session);
    }

    public static String getReferencesToSchema(Connection conn,
            String schemaName) {

        Session session = (Session) ((JDBCConnection) conn).getSession();
        SchemaManager     sm     = session.getDatabase().schemaManager;
        MultiValueHashMap map    = sm.getReferencesToSchema(schemaName);
        StringBuffer      sb     = new StringBuffer();
        Iterator          mainIt = map.keySet().iterator();

        while (mainIt.hasNext()) {
            HsqlName name = (HsqlName) mainIt.next();

            if (name.parent != null) {
                sb.append('(');
                sb.append(name.parent.getSchemaQualifiedStatementName());
                sb.append(')');
            }

            sb.append(name.getSchemaQualifiedStatementName());
            sb.append('=');

            Iterator it = map.get(name);

            while (it.hasNext()) {
                name = (HsqlName) it.next();

                if (name.parent != null) {
                    sb.append('(');
                    sb.append(name.parent.getSchemaQualifiedStatementName());
                    sb.append(')');
                }

                sb.append(name.getSchemaQualifiedStatementName());
                sb.append(',');
            }

            sb.setCharAt(sb.length() - 1, ' ');
            sb.append(' ');
        }

        return sb.toString();
    }
}
/*
    CREATE FUNCTION BLOB_LENGTH(LOB BLOB) RETURNS BIGINT
    LANGUAGE JAVA EXTERNAL NAME 'CLASSPATH:org.hsqldb.lib.UtilFunctions.getBlobLength'

    CREATE FUNCTION DELETED_BLOB(BLOCK_ID INT, BL_COUNT INT) RETURNS VARBINARY(1000000)
    LANGUAGE JAVA EXTERNAL NAME 'CLASSPATH:org.hsqldb.lib.UtilFunctions.getDeletedBlob'

    select resourceid, lob_id(data), blob_length(data) from project_resources where  blob_length(data) < 0

    select resourceid,  lob_id(data), blob_length(data) from project_resources
    where  lob_id(data) not in (select lob_ids.LOB_id from SYSTEM_LOBS.LOB_IDS)

    update project_resources set data = null where  blob_length(data) < 0

    // system_lobs.blocks contains address and size for each previously deleted block
    select block_addr, block_count, deleted_blob(block_addr, block_count) from system_lobs.blocks where block_count < 10 order by block_addr limit 10


*/
