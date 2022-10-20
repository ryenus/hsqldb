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


package org.hsqldb.lib;

import org.hsqldb.map.BaseHashMap;

/**
 * A list which is also a set of long primitives which maintains the insertion
 * order of the elements and allows access by index. Iterators return the keys
 * or values in the index order.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.0
 * @since 1.9.0
 */
public class OrderedLongHashSet extends BaseHashMap {

    public OrderedLongHashSet() {
        this(8);
    }

    public OrderedLongHashSet(int initialCapacity)
    throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.noKeyOrValue, false);

        isList = true;
    }

    public boolean contains(long key) {
        return super.containsKey(key);
    }

    public boolean add(long key) {

        int oldSize = size();

        super.addOrUpdate(key, 0, null, null);

        return oldSize != size();
    }

    public boolean insert(int index,
                          long key) throws IndexOutOfBoundsException {

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }

        if (contains(key)) {
            return false;
        }

        if (index < size()) {
            super.insertRow(index);
        }

        return add(key);
    }

    public boolean remove(long key) {

        return (Boolean) super.remove(key, 0, null, null, false, true);
    }

    public void removeEntry(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        long key = longKeyTable[index];

        super.remove(key, 0, null, null, false, true);
    }

    public long get(int index) {

        checkRange(index);

        return longKeyTable[index];
    }

    public int getIndex(long value) {
        return getLookup(value);
    }

    public int getStartMatchCount(long[] array) {

        int i = 0;

        for (; i < array.length; i++) {
            if (!super.containsKey(array[i])) {
                break;
            }
        }

        return i;
    }

    public int getOrderedStartMatchCount(long[] array) {

        int i = 0;

        for (; i < array.length; i++) {
            if (i >= size() || get(i) != array[i]) {
                break;
            }
        }

        return i;
    }

    public boolean addAll(OrderedLongHashSet col) {

        int oldSize = size();

        for (int i = 0; i < oldSize; i++) {
            long val = col.longValueTable[i];

            add(val);
        }

        return oldSize != size();
    }

    public long[] toArray() {

        int    lookup = -1;
        long[] array  = new long[size()];

        for (int i = 0; i < array.length; i++) {
            lookup = super.nextLookup(lookup);

            long value = intKeyTable[lookup];

            array[i] = value;
        }

        return array;
    }

    public PrimitiveIterator<Long> iterator() {
        return new BaseHashIterator(true);
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
