/* Copyright (c) 2001-2021, The HSQL Development Group
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

import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 2.5.1
 */
public class DoubleIntArrayCachedObject extends CachedObjectBase {

    public static final int fileSizeFactor = 8;

    //
    DoubleIntIndex table;

    public DoubleIntArrayCachedObject(int capacity) {
        this.table = new DoubleIntIndex(capacity, true);
        hasChanged = true;
    }

    public void read(RowInputInterface in) {

        this.position = in.getFilePosition();

        int   capacity       = table.capacity();
        int[] array          = table.getKeys();
        int   lastValueIndex = -1;

        for (int i = 0; i < capacity; i++) {
            array[i] = in.readInt();

            if (array[i] != 0) {
                lastValueIndex = i;
            }
        }

        array = table.getValues();

        for (int i = 0; i < capacity; i++) {
            array[i] = in.readInt();
        }

        table.setSize(lastValueIndex + 1);

        hasChanged = false;
    }

    public int getDefaultCapacity() {
        return table.capacity();
    }

    public int getRealSize(RowOutputInterface out) {
        return table.capacity() * fileSizeFactor;
    }

    public void write(RowOutputInterface out) {
        write(out, null);
    }

    public void write(RowOutputInterface out, LongLookup lookup) {

        int capacity = table.capacity();

        out.setStorageSize(storageSize);

        int[] array = table.getKeys();

        for (int i = 0; i < capacity; i++) {
            out.writeInt(array[i]);
        }

        array = table.getValues();

        for (int i = 0; i < capacity; i++) {
            out.writeInt(array[i]);
        }

        out.writeEnd();
    }

    public void clear() {

        hasChanged |= table.size() > 0;

        table.clear();
    }

    public boolean removeKey(int key) {

        boolean result = table.removeKey(key);

        hasChanged |= result;

        return result;
    }

    public boolean addKey(int key, int value) {

        boolean result = table.addOrReplaceUnique(key, value);

        hasChanged |= result;

        return result;
    }

    /**
     * assumes all values are zero or positive
     */
    public int getValue(int key) {
        return table.lookup(key, -1);
    }

    public int getValue(int key, int def) {
        return table.lookup(key, def);
    }
}
