/* Copyright (c) 2001-2025, The HSQL Development Group
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

import org.hsqldb.Row;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/*
 * All-in-memory implementation of RowSetNavigator for client side, or for
 * transferring a slice of the result to the client or server using a subset of
 * a server-side row set.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.9.0
 */
public class RowSetNavigatorClient extends RowSetNavigator {

    public static final Object[][] emptyTable = new Object[0][];

    //
    int currentOffset;
    int baseBlockSize;

    //
    Object[][] table;

    //
    public RowSetNavigatorClient() {
        table = emptyTable;
    }

    public RowSetNavigatorClient(int blockSize) {
        table = new Object[blockSize][];
    }

    public RowSetNavigatorClient(
            RowSetNavigator source,
            int offset,
            int blockSize) {

        this.size          = source.size;
        this.baseBlockSize = blockSize;
        this.currentOffset = offset;
        table              = new Object[blockSize][];

        source.absolute(offset);

        for (int count = 0; count < blockSize; count++) {
            table[count] = source.getCurrent();

            source.next();
        }
    }

    /**
     * For communication of small results such as BATCHEXECRESPONSE
     */
    public void setData(Object[][] table) {
        this.table = table;
        this.size  = table.length;
    }

    public void setData(int pos, Object[] data) {

        if (pos < 0 || pos >= size || pos >= currentOffset + table.length) {
            return;
        }

        if (pos < currentOffset) {
            return;
        }

        table[pos - currentOffset] = data;
    }

    public Object[] getData(int index) {
        return table[index];
    }

    /**
     * Returns the current row data.
     */
    public Object[] getCurrent() {

        if (currentPos < 0 || currentPos >= size) {
            return null;
        }

        if (currentPos >= currentOffset + table.length
                || currentPos < currentOffset) {
            int newOffset = (currentPos / baseBlockSize) * baseBlockSize;

            getBlock(newOffset);
        }

        return table[currentPos - currentOffset];
    }

    public Row getCurrentRow() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigatorClient");
    }

    /**
     * Only for navigators for INSERT
     */
    public void removeCurrent() {

        System.arraycopy(
            table,
            currentPos + 1,
            table,
            currentPos,
            size - currentPos - 1);

        table[size - 1] = null;

        currentPos--;
        size--;
    }

    public void add(Object[] data) {

        ensureCapacity();

        table[size] = data;

        size++;
    }

    public void clear() {

        setData(emptyTable);

        size = 0;

        reset();
    }

    public void release() {

        setData(emptyTable);
        reset();

        isClosed = true;
    }

    public boolean absolute(int position) {

        if (position < 0) {
            beforeFirst();

            return false;
        }

        if (position >= size) {
            afterLast();

            return false;
        }

        if (size == 0) {
            return false;
        }

        currentPos = position;

        return true;
    }

    public void readSimple(RowInputInterface in, ResultMetaData meta) {

        size = in.readInt();

        if (table.length < size) {
            table = new Object[size][];
        }

        for (int i = 0; i < size; i++) {
            table[i] = in.readData(meta.columnTypes);
        }
    }

    public void writeSimple(RowOutputInterface out, ResultMetaData meta) {

        out.writeInt(size);

        for (int i = 0; i < size; i++) {
            Object[] data = table[i];

            out.writeData(
                meta.getColumnCount(),
                meta.columnTypes,
                data,
                null,
                null);
        }
    }

    public void read(RowInputInterface in, ResultMetaData meta) {

        id            = in.readLong();
        size          = in.readInt();
        currentOffset = in.readInt();
        baseBlockSize = in.readInt();

        if (table.length < baseBlockSize) {
            table = new Object[baseBlockSize][];
        }

        for (int i = 0; i < baseBlockSize; i++) {
            table[i] = in.readData(meta.columnTypes);
        }
    }

    public void write(RowOutputInterface out, ResultMetaData meta) {

        int limit = size - currentOffset;

        if (limit > table.length) {
            limit = table.length;
        }

        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(currentOffset);
        out.writeInt(limit);

        for (int i = 0; i < limit; i++) {
            Object[] data = table[i];

            out.writeData(
                meta.getExtendedColumnCount(),
                meta.columnTypes,
                data,
                null,
                null);
        }
    }

    /**
     * baseBlockSize remains unchanged.
     */
    void getBlock(int offset) {

        try {
            RowSetNavigatorClient source = session.getRows(
                id,
                offset,
                baseBlockSize);

            table         = source.table;
            currentOffset = source.currentOffset;
        } catch (HsqlException e) {
            throw e;
        }
    }

    private void ensureCapacity() {

        if (size == table.length) {
            int        newSize  = size == 0
                                  ? 4
                                  : size * 2;
            Object[][] newTable = new Object[newSize][];

            System.arraycopy(table, 0, newTable, 0, size);

            table = newTable;
        }
    }
}
