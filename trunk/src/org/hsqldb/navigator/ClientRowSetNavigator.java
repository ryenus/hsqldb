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


package org.hsqldb.navigator;

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.Trace;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/*
 * All-in-memory implementation of RowSetNavigator for client side, or for
 * transferring a slice of the result to the client using a subset of a
 * server-side row set.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class ClientRowSetNavigator extends RowSetNavigator {

    int currentOffset;
    int baseBlockSize;

    //
    Object[][] table;

    //
    public ClientRowSetNavigator() {}

    public ClientRowSetNavigator(RowSetNavigator source, int offset,
                                 int blockSize) {

        this.size          = source.size;
        this.baseBlockSize = blockSize;
        this.currentOffset = offset;
        table              = new Object[blockSize][];

        source.absolute(offset);

        for (int count = 0; count < blockSize; count++) {
            table[count] = (Object[]) source.getCurrent();

            source.next();
        }

        source.beforeFirst();
    }

    /**
     * For communication of small resuls such as BATCHEXECRESPONSE
     */
    public void setData(Object[][] table) {
        this.table = table;
        this.size  = table.length;
    }

    /**
     * Returns the current row object. Type of object is implementation defined.
     */
    public Object getCurrent() {

        if (currentPos < 0 || currentPos >= size) {
            return null;
        }

        if (currentPos == currentOffset + table.length) {
            getBlock(currentOffset + table.length);
        }

        return table[currentPos - currentOffset];
    }

    public void remove() {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "ClientRowSetNavigator");
    }

    public void add(Object data) {
        Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                           "ClientRowSetNavigator");
    }

    public void clear() {
        Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                           "ClientRowSetNavigator");
    }

    public boolean absolute(int position) {

        if (position < 1) {
            position += size;
        }

        if (position < 0) {
            beforeFirst();

            return false;
        }

        if (position > size) {
            afterLast();

            return false;
        }

        if (size == 0) {
            return false;
        }

        currentPos = position;

        return true;
    }

    public void close() {

        if (session != null) {
            if (currentOffset == 0 && table.length == size) {}
            else {
                session.closeNavigator(id);
            }
        }
    }

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws HsqlException, IOException {

        id            = in.readLong();
        size          = in.readInt();
        currentOffset = in.readInt();
        baseBlockSize = in.readInt();
        table         = new Object[baseBlockSize][];

        for (int i = 0; i < baseBlockSize; i++) {
            table[i] = in.readData(meta.colTypes);
        }
    }

    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws HsqlException, IOException {

        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(currentOffset);
        out.writeInt(table.length);

        for (int i = 0; i < table.length; i++) {
            Object[] data = table[i];

            out.writeData(meta.getColumnCount(), meta.colTypes, data, null,
                          null);
        }
    }

    /**
     * baseBlockSize remains unchanged.
     */
    void getBlock(int offset) {

        try {
            ClientRowSetNavigator source = session.getRows(id, offset,
                baseBlockSize);

            table         = source.table;
            currentOffset = source.currentOffset;
        } catch (HsqlException e) {}
    }
}
