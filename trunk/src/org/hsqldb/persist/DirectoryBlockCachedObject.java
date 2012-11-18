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


package org.hsqldb.persist;

import java.io.IOException;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.3.0
 */
public class DirectoryBlockCachedObject extends CachedObjectBase {

    public final static int fileSizeFactor = 10;

    //
    int[]  tableIds;
    int[]  bitmapAddress;
    char[] freeSpace;

    public DirectoryBlockCachedObject(int capacity) {

        this.tableIds      = new int[capacity];
        this.bitmapAddress = new int[capacity];
        this.freeSpace     = new char[capacity];
        hasChanged         = true;
    }

    public CachedObject newInstance(int size) {
        return new DirectoryBlockCachedObject(size);
    }

    public void read(RowInputInterface in) {

        this.position = in.getPos();

        int capacity = tableIds.length;

        try {
            for (int i = 0; i < capacity; i++) {
                tableIds[i] = in.readInt();
            }

            for (int i = 0; i < capacity; i++) {
                bitmapAddress[i] = in.readInt();
            }

            for (int i = 0; i < capacity; i++) {
                freeSpace[i] = in.readChar();
            }
        } catch (IOException e) {
            throw Error.error(ErrorCode.GENERAL_IO_ERROR, e);
        }

        hasChanged = false;
    }

    public int getDefaultCapacity() {
        return tableIds.length;
    }

    public int getRealSize(RowOutputInterface out) {

        return tableIds.length
               * (PersistentStore.INT_STORE_SIZE * 2
                  + PersistentStore.SHORT_STORE_SIZE);
    }

    public void write(RowOutputInterface out) {

        int capacity = tableIds.length;

        for (int i = 0; i < capacity; i++) {
            out.writeInt(tableIds[i]);
        }

        for (int i = 0; i < capacity; i++) {
            out.writeInt(bitmapAddress[i]);
        }

        for (int i = 0; i < capacity; i++) {
            out.writeChar(freeSpace[i]);
        }

        out.writeEnd();

        hasChanged = false;
    }

    public void write(RowOutputInterface out, LongLookup lookup) {
        write(out);
    }

    public int[] getTableIdArray() {
        return tableIds;
    }

    public int[] getBitmapAddressArray() {
        return bitmapAddress;
    }

    public char[] getFreeSpaceArray() {
        return freeSpace;
    }
}
