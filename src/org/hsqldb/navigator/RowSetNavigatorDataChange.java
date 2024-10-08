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


package org.hsqldb.navigator;

import java.io.IOException;

import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Type;

/*
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 2.2.7
 */
public interface RowSetNavigatorDataChange extends RangeIterator {

    int getMainRowCount();

    int getSize();

    int getRowPosition();

    boolean beforeFirst();

    Object[] getCurrentChangedData();

    int[] getCurrentChangedColumns();

    default void setCurrentChangedColumns(int[] colMap) {}

    default void write(RowOutputInterface out, ResultMetaData meta) {}

    default void read(RowInputInterface in, ResultMetaData meta) {}

    void endMainDataSet();

    boolean addRow(Row row);

    Object[] addRow(
            Session session,
            Row row,
            Object[] data,
            Type[] types,
            int[] columnMap);

    default void addUpdatedRow(Row row) {}

    default Row getUpdatedRow() {
        return null;
    }

    boolean addUpdate(Row row, Object[] data, int[] columnMap);

    boolean containsDeletedRow(Row row);

    boolean containsUpdatedRow(Row row, Row refRow, int[] keys);

    RangeIterator getUpdateRowIterator();
}
