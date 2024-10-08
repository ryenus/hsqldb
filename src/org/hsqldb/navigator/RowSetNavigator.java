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

import org.hsqldb.Row;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Encapsulates navigation functionality for lists of objects. The base class
 * provides positional navigation and checking, while the subclasses provide
 * object retrieval.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.9.0
 */
public abstract class RowSetNavigator implements RangeIterator {

    public RowSetNavigator() {}

    SessionInterface session;
    long             id;
    int              size;
    int              mode;
    int              currentPos = -1;
    int              rangePosition;
    boolean          isClosed;

    /**
     * Sets the id;
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Returns the id
     */
    public long getId() {
        return id;
    }

    /**
     * Returns the current row object. Type of object is implementation defined.
     */
    public abstract Object[] getCurrent();

    public Object getField(int col) {

        Object[] current = getCurrent();

        if (current == null) {
            return null;
        }

        return current[col];
    }

    public void setCurrent(Object[] data) {}

    public abstract Row getCurrentRow();

    /**
     * Add data to the end
     */
    public abstract void add(Object[] data);

    /**
     * Remove current row
     */
    public abstract void removeCurrent();

    /**
     * Reset to initial state
     */
    public void reset() {
        currentPos = -1;
    }

    /**
     * Clear the contents
     */
    public abstract void clear();

    /**
     * Remove any resources and invalidate
     */
    public abstract void release();

    public boolean isClosed() {
        return isClosed;
    }

    public void setSession(SessionInterface session) {
        this.session = session;
    }

    public SessionInterface getSession() {
        return session;
    }

    public int getSize() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean next() {

        if (hasNext()) {
            currentPos++;

            return true;
        } else if (size != 0) {
            currentPos = size;
        }

        return false;
    }

    final boolean hasNext() {
        return currentPos < size - 1;
    }

    public long getRowId() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }

    public Object[] getData(long rowId) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }

    public boolean beforeFirst() {

        reset();

        currentPos = -1;

        return true;
    }

    public boolean afterLast() {

        if (size == 0) {
            return false;
        }

        reset();

        currentPos = size;

        return true;
    }

    public boolean first() {
        beforeFirst();

        return next();
    }

    public boolean last() {

        if (size == 0) {
            return false;
        }

        if (isAfterLast()) {
            beforeFirst();
        }

        while (hasNext()) {
            next();
        }

        return true;
    }

    public int getRowNumber() {
        return currentPos;
    }

    /**
     * Uses similar semantics to java.sql.ResultSet except this is 0 based.
     * When position is 0 or positive, it is from the start; when negative,
     * it means beforeFirst()
     */
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

        if (position < currentPos) {
            beforeFirst();
        }

        // go to the target row;
        while (position > currentPos) {
            next();
        }

        return true;
    }

    public boolean relative(int rows) {

        int position = currentPos + rows;

        if (position < 0) {
            beforeFirst();

            return false;
        }

        return absolute(position);
    }

    public boolean previous() {
        return relative(-1);
    }

    public boolean isFirst() {
        return size > 0 && currentPos == 0;
    }

    public boolean isLast() {
        return size > 0 && currentPos == size - 1;
    }

    public boolean isBeforeFirst() {
        return size > 0 && currentPos == -1;
    }

    public boolean isAfterLast() {
        return size > 0 && currentPos == size;
    }

    public void writeSimple(RowOutputInterface out, ResultMetaData meta) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }

    public void readSimple(RowInputInterface in, ResultMetaData meta) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }

    public abstract void write(RowOutputInterface out, ResultMetaData meta);

    public abstract void read(RowInputInterface in, ResultMetaData meta);

    public boolean isMemory() {
        return true;
    }

    public int getRangePosition() {
        return rangePosition;
    }
}
