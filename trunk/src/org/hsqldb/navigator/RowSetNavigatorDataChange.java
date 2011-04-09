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


package org.hsqldb.navigator;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedLongKeyHashMap;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Type;

/*
 * All-in-memory implementation of RowSetNavigator for delete and update
 * operations.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.1.1
 * @since 1.9.0
 */
public class RowSetNavigatorDataChange extends RowSetNavigator {

    OrderedLongKeyHashMap list;
    boolean               enforceDeleteOrUpdate;
    boolean               enforceSingleUpdate;

    public RowSetNavigatorDataChange() {
        list = new OrderedLongKeyHashMap(8, true);
    }

    public RowSetNavigatorDataChange(boolean enforceDeleteOrUpdate,
                                     boolean enforceSingleUpdate) {

        list                       = new OrderedLongKeyHashMap(8, true);
        this.enforceDeleteOrUpdate = enforceDeleteOrUpdate;
        this.enforceSingleUpdate   = enforceSingleUpdate;
    }

    /**
     * Returns the current row object.
     */
    public Object[] getCurrent() {
        return getCurrentRow().getData();
    }

    public Row getCurrentRow() {
        return (Row) list.getValueByIndex(super.currentPos);
    }

    public Object[] getCurrentChangedData() {
        return (Object[]) list.getSecondValueByIndex(super.currentPos);
    }

    public int[] getCurrentChangedColumns() {
        return (int[]) list.getThirdValueByIndex(super.currentPos);
    }

    public Row getNextRow() {
        return next() ? getCurrentRow()
                      : null;
    }

    public void remove() {
        throw new NoSuchElementException();
    }

    public boolean next() {
        return super.next();
    }

    public void reset() {
        super.reset();
    }

    // reading and writing
    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws IOException {

        beforeFirst();
        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(0);    // offset
        out.writeInt(size);

        while (hasNext()) {
            Object[] data = getNext();

            out.writeData(meta.getColumnCount(), meta.columnTypes, data, null,
                          null);
        }

        beforeFirst();
    }

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws IOException {

        id = in.readLong();

        int count = in.readInt();

        in.readInt();    // offset
        in.readInt();    // size again

        while (count-- > 0) {
            add(in.readData(meta.columnTypes));
        }
    }

    public void clear() {

        reset();
        list.clear();

        size = 0;
    }

    public void add(Object[] d) {
        throw Error.runtimeError(ErrorCode.U_S0500,
                                 "RowSetNavigatorDataChange");
    }

    public boolean addRow(Row row) {

        int lookup = list.getLookup(row.getId());

        if (lookup == -1) {
            list.put(row.getId(), row, null);

            size++;

            return true;
        } else {
            if (list.getSecondValueByIndex(lookup) != null) {
                if (enforceDeleteOrUpdate) {
                    throw Error.error(ErrorCode.X_27000);
                }

                list.setSecondValueByIndex(lookup, null);
                list.setThirdValueByIndex(lookup, null);

                return true;
            }

            return false;
        }
    }

    public Object[] addRow(Session session, Row row, Object[] data,
                           Type[] types, int[] columnMap) {

        long rowId  = row.getId();
        int  lookup = list.getLookup(rowId);

        if (lookup == -1) {
            list.put(rowId, row, data);
            list.setThirdValueByIndex(size, columnMap);

            size++;

            return data;
        } else {
            Object[] rowData = ((Row) list.getFirstByLookup(lookup)).getData();
            Object[] currentData =
                (Object[]) list.getSecondValueByIndex(lookup);

            if (currentData == null) {
                if (enforceDeleteOrUpdate) {
                    throw Error.error(ErrorCode.X_27000);
                } else {
                    return null;
                }
            }

            for (int i = 0; i < columnMap.length; i++) {
                int j = columnMap[i];

                if (types[j].compare(session, data[j], currentData[j]) != 0) {
                    if (types[j].compare(session, rowData[j], currentData[j])
                            != 0) {
                        if (enforceSingleUpdate) {
                            throw Error.error(ErrorCode.X_27000);
                        }
                    } else {
                        currentData[j] = data[j];
                    }
                }
            }

            int[] currentMap = (int[]) list.getThirdValueByIndex(lookup);

            currentMap = ArrayUtil.union(currentMap, columnMap);

            list.setThirdValueByIndex(lookup, currentMap);

            return currentData;
        }
    }

    public boolean containsDeletedRow(Row row) {

        int lookup = list.getLookup(row.getId());

        if (lookup == -1) {
            return false;
        }

        Object[] currentData = (Object[]) list.getSecondValueByIndex(lookup);

        return currentData == null;
    }
}
